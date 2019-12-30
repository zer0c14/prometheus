// Copyright 2019 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importer

import (
	"bufio"
	"bytes"
	"context"
	"encoding/gob"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/textparse"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"strings"
)

// Implementing the error interface to create a
// constant, which cannot be overridden.
// https://dave.cheney.net/2016/04/07/constant-errors
type Error string

func (e Error) Error() string {
	return string(e)
}

const (
	// This error is thrown when the current entry does not have a timestamp associated.
	NoTimestampError = Error("expected timestamp with metric")
	// This error is thrown when the sample being parsed currently aligns with the database's
	// time ranges, but we cannot find a suitable block for it.
	NoBlockFoundError = Error("no corresponding block found for current sample")
	// This error is thrown when the sample being parsed currently has a corresponding block
	// in the database, but has no metadata collected for it, curiously enough.
	NoBlockMetaFoundError = Error("no metadata found for current samples' block")
)

type blockTimestampPair struct {
	start, end int64
}

type newBlockMeta struct {
	index      int
	count      int
	mint, maxt int64
	isAligned  bool
	dir        string
	children   []newBlockMeta
}

// Content Type for the Open Metrics Parser.
// Needed to init the text parser.
const contentType = "application/openmetrics-text;"

// ImportFromFile imports data from a file formatted according to the Open Metrics format,
// converts it into block(s), and places the newly created block(s) in the
// TSDB DB directory, where it is treated like any other block.
func ImportFromFile(filePath string, dbPath string, maxSamplesInMemory int, logger log.Logger) error {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	level.Debug(logger).Log("msg", "creating temp directory")
	tmpDbDir, err := ioutil.TempDir("", "importer")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDbDir)

	level.Debug(logger).Log("msg", "reading existing block metadata to infer time ranges")
	dbMint, dbMaxt, blockTimes, err := getDbTimes(dbPath)
	if err != nil {
		return err
	}

	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	level.Debug(logger).Log("msg", "creating new block skeletons based on existing blocks")
	blockMetas := constructBlockMetadata(dbMint, dbMaxt, blockTimes)

	level.Info(logger).Log("msg", "importing input data and aligning with existing DB")
	if err = writeSamples(f, tmpDbDir, dbMint, dbMaxt, blockMetas, maxSamplesInMemory, logger); err != nil {
		return err
	}

	level.Info(logger).Log("msg", "merging fully overlapping blocks")
	if err = mergeBlocks(tmpDbDir, blockMetas, logger); err != nil {
		return err
	}

	level.Debug(logger).Log("msg", "copying newly created blocks from temp location to actual DB location")
	if err = fileutil.CopyDirs(tmpDbDir, dbPath); err != nil {
		return err
	}

	return nil
}

// writeSamples parses each metric sample, and writes the samples to the correspondingly aligned block.
// It uses the block indexes assigned to each sample, and block meta info, gathered in collectSampleInformation().
func writeSamples(f *os.File, dbDir string, dbMint, dbMaxt int64, blockMetas []*newBlockMeta, maxSamplesInMemory int, logger log.Logger) error {
	blocks := getEmptyBlocks(len(blockMetas))
	sampleCount := 0
	currentPassCount := 0

	encBuf := new(bytes.Buffer)
	streamSamples := func(data []byte, atEOF bool) (int, []byte, error) {
		var err error
		advance := 0
		lineIndex := 0
		lines := strings.Split(string(data), "\n")
		parser := textparse.New(data, contentType)
		for {
			var et textparse.Entry
			if et, err = parser.Next(); err != nil {
				if !atEOF && et == textparse.EntryInvalid {
					return 0, nil, nil
				}
				if err == io.EOF {
					err = nil
				}
				return 0, nil, err
			}

			// Add 1 to account for newline.
			lineLength := len(lines[lineIndex]) + 1
			lineIndex++

			if et != textparse.EntrySeries {
				advance = advance + lineLength
				continue
			}

			_, ctime, cvalue := parser.Series()
			if ctime == nil {
				return 0, nil, NoTimestampError
			} else {
				// OpenMetrics parser multiples times by 1000 - undoing that.
				ctimeCorrected := *ctime / 1000

				var clabels labels.Labels
				_ = parser.Metric(&clabels)

				sample := tsdb.MetricSample{
					Timestamp: ctimeCorrected,
					Value:     cvalue,
					Labels:    labels.FromMap(clabels.Map()),
				}

				encBuf.Reset()
				enc := gob.NewEncoder(encBuf)
				err = enc.Encode(sample)
				if err != nil {
					return 0, nil, err
				}

				advance += lineLength
				return advance, encBuf.Bytes(), nil
			}
		}
	}

	// Use a streaming approach to avoid loading too much data at once.
	scanner := bufio.NewScanner(f)
	scanner.Split(streamSamples)

	for scanner.Scan() {
		if currentPassCount == 0 {
			blocks = getEmptyBlocks(len(blockMetas))
		}

		encSample := scanner.Bytes()
		decBuf := bytes.NewBuffer(encSample)
		sample := tsdb.MetricSample{}
		if err := gob.NewDecoder(decBuf).Decode(&sample); err != nil {
			level.Error(logger).Log("msg", "failed to decode current entry returned by file scanner", "err", err)
			return err
		}

		// Find what block does this sample belong to.
		blockIndex := -1
		if len(blockMetas) == 1 || sample.Timestamp < dbMint {
			blockIndex = 0
		} else if sample.Timestamp >= dbMaxt {
			blockIndex = len(blockMetas) - 1
		} else {
			for blockIdx, meta := range blockMetas {
				if meta.mint <= sample.Timestamp && sample.Timestamp < meta.maxt {
					blockIndex = blockIdx
					break
				}
			}
		}

		if blockIndex < 0 || blockIndex >= len(blockMetas) {
			return NoBlockMetaFoundError
		}
		meta := blockMetas[blockIndex]
		meta.mint = value.MinInt64(meta.mint, sample.Timestamp)
		meta.maxt = value.MaxInt64(meta.maxt, sample.Timestamp)
		meta.count += 1

		blocks[blockIndex] = append(blocks[blockIndex], &sample)

		currentPassCount += 1
		sampleCount += 1
		// Have enough samples to write to disk.
		if currentPassCount == maxSamplesInMemory {
			if err := flushBlocks(dbDir, blocks, blockMetas, logger); err != nil {
				return err
			}
			// Reset current pass count.
			currentPassCount = 0
		}
	}

	// Flush any remaining samples.
	if err := flushBlocks(dbDir, blocks, blockMetas, logger); err != nil {
		return err
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

// flushBlocks writes the given blocks of samples to disk, and updates block metadata information.
func flushBlocks(dbDir string, blocksToFlush [][]*tsdb.MetricSample, blockMetas []*newBlockMeta, logger log.Logger) error {
	for blockIdx, block := range blocksToFlush {
		// If current block is empty, nothing to write.
		if len(block) == 0 {
			continue
		}
		meta := blockMetas[blockIdx]
		// Put each sample into the appropriate block.
		bins, binTimes := binSamples(block, meta, tsdb.DefaultBlockDuration)
		for binIdx, bin := range bins {
			if len(bin) == 0 {
				continue
			}
			start, end := binTimes[binIdx].start, binTimes[binIdx].end
			path, err := tsdb.CreateBlock(bin, dbDir, start, end, logger)
			if err != nil {
				return err
			}
			child := newBlockMeta{
				index:     blockIdx,
				count:     0,
				mint:      start,
				maxt:      end,
				isAligned: meta.isAligned,
				dir:       path,
			}
			meta.children = append(meta.children, child)
		}
	}
	return nil
}

// makeRange returns a series of block times between start and stop,
// with step divisions.
func makeRange(start, stop, step int64) []blockTimestampPair {
	if step <= 0 || stop < start {
		return []blockTimestampPair{}
	}
	r := make([]blockTimestampPair, 0)
	// In case we only have samples with the same timestamp.
	if start == stop {
		r = append(r, blockTimestampPair{
			start: start,
			end:   stop + 1,
		})
		return r
	}
	for s := start; s < stop; s += step {
		pair := blockTimestampPair{
			start: s,
		}
		if (s + step) >= stop {
			pair.end = stop + 1
		} else {
			pair.end = s + step
		}
		r = append(r, pair)
	}
	return r
}

// getEmptyBlocks is a simple helper function to create bins for samples.
func getEmptyBlocks(num int) [][]*tsdb.MetricSample {
	blocks := make([][]*tsdb.MetricSample, 0)
	for idx := 0; idx < num; idx++ {
		block := make([]*tsdb.MetricSample, 0)
		blocks = append(blocks, block)
	}
	return blocks
}

// mergeBlocks looks for blocks that have overlapping time intervals, and compacts them.
func mergeBlocks(dbDir string, blockMetas []*newBlockMeta, logger log.Logger) error {
	for _, blockMeta := range blockMetas {
		// If no children, there's nothing to merge.
		if len(blockMeta.children) == 0 {
			continue
		}
		if blockMeta.isAligned {
			dirs := make([]string, 0)
			for _, meta := range blockMeta.children {
				dirs = append(dirs, meta.dir)
			}
			path, err := compactBlocks(dbDir, dirs, logger)
			if err != nil {
				return err
			}
			blockMeta.dir = path
		} else {
			binned := binBlocks(blockMeta.children, tsdb.DefaultBlockDuration)
			for _, bin := range binned {
				if len(bin) <= 1 {
					continue
				}
				dirs := make([]string, 0)
				for _, meta := range bin {
					dirs = append(dirs, meta.dir)
				}
				path, err := compactBlocks(dbDir, dirs, logger)
				if err != nil {
					return err
				}
				for _, dir := range dirs {
					if err = os.RemoveAll(dir); err != nil {
						return err
					}
				}
				for _, meta := range bin {
					meta.dir = path
				}
			}
		}
	}
	return nil
}

// compactBlocks compacts the block dirs and places them in dest.
func compactBlocks(dest string, dirs []string, logger log.Logger) (string, error) {
	path := ""
	compactor, err := tsdb.NewLeveledCompactor(context.Background(), nil, logger, tsdb.DefaultOptions.BlockRanges, nil)
	if err != nil {
		return path, err
	}
	ulid, err := compactor.Compact(dest, dirs, nil)
	if err != nil {
		return path, err
	}
	return filepath.Join(dest, ulid.String()), nil
}

// binBlocks groups blocks that have fully intersecting intervals of the given duration.
// Input blocks are assumed sorted by time.
func binBlocks(blocks []newBlockMeta, duration int64) [][]newBlockMeta {
	bins := make([][]newBlockMeta, 0)
	if len(blocks) == 0 {
		return bins
	}

	bin := make([]newBlockMeta, 0)

	var start int64
	for blockIdx, block := range blocks {
		if blockIdx == 0 {
			start = block.mint
		}
		if block.maxt-block.mint >= duration {
			if block.mint <= start {
				bin = append(bin, block)
				bins = append(bins, bin)
			} else {
				bins = append(bins, bin)
				bins = append(bins, []newBlockMeta{block})
			}
			bin = []newBlockMeta{}
			start = block.maxt
			continue
		}
		if block.mint-start < duration && block.maxt-start < duration {
			bin = append(bin, block)

		} else {
			bins = append(bins, bin)
			bin = []newBlockMeta{block}
			start = block.mint
		}
	}
	bins = append(bins, bin)
	return bins
}

// binSamples divvies the samples into bins corresponding to the given block.
// If an aligned block is given to it, then a single bin is created,
// else, it divides the samples into blocks of duration.
// It returns the binned samples, and the time limits for each bin.
func binSamples(samples []*tsdb.MetricSample, meta *newBlockMeta, duration int64) ([][]*tsdb.MetricSample, []blockTimestampPair) {
	findBlock := func(ts int64, blocks []blockTimestampPair) int {
		for idx, block := range blocks {
			if block.start <= ts && ts < block.end {
				return idx
			}
		}
		return -1
	}
	bins := make([][]*tsdb.MetricSample, 0)
	times := make([]blockTimestampPair, 0)
	if len(samples) == 0 {
		return bins, times
	}
	if meta.isAligned {
		bins = append(bins, samples)
		times = []blockTimestampPair{{start: meta.mint, end: meta.maxt}}
	} else {
		timeTranches := makeRange(meta.mint, meta.maxt, duration)
		bins = getEmptyBlocks(len(timeTranches))
		binIdx := -1
		for _, sample := range samples {
			ts := sample.Timestamp
			if binIdx == -1 {
				binIdx = findBlock(ts, timeTranches)
			}
			block := timeTranches[binIdx]
			if block.start <= ts && ts < block.end {
				bins[binIdx] = append(bins[binIdx], sample)
			} else {
				binIdx = findBlock(ts, timeTranches)
				bins[binIdx] = append(bins[binIdx], sample)
			}
		}
		times = timeTranches
	}
	return bins, times
}

// constructBlockMetadata gives us the new blocks' metadatas.
func constructBlockMetadata(dbMint, dbMaxt int64, blockTimes []blockTimestampPair) []*newBlockMeta {
	blockMetas := make([]*newBlockMeta, 0)
	if len(blockTimes) == 0 {
		// If no blocks are found, then just create one block.
		blockMetas = append(blockMetas, &newBlockMeta{
			index:     0,
			count:     0,
			mint:      math.MaxInt64,
			maxt:      dbMint,
			isAligned: false,
		})
	} else {
		// We have at least 2 block metas - before dbMint, and after dbMaxt.
		blockMetas = append(blockMetas, &newBlockMeta{
			index:     0,
			count:     0,
			mint:      math.MaxInt64,
			maxt:      dbMint,
			isAligned: false,
		})
		for idx, block := range blockTimes {
			blockMetas = append(blockMetas, &newBlockMeta{
				index:     idx + 1,
				count:     0,
				mint:      block.start,
				maxt:      block.end,
				isAligned: true,
			})
		}
		blockMetas = append(blockMetas, &newBlockMeta{
			index:     len(blockTimes) + 1,
			count:     0,
			mint:      dbMaxt,
			maxt:      math.MinInt64,
			isAligned: false,
		})
	}
	return blockMetas
}

// getDbTimes returns the DB's min, max timestamps, and each individual blocks'
// timestamps.
func getDbTimes(dbPath string) (int64, int64, []blockTimestampPair, error) {
	var blockTimes []blockTimestampPair
	mint := int64(math.MinInt64)
	maxt := int64(math.MaxInt64)
	// If we try to open a regular RW handle on an active TSDB instance,
	// it will fail. Hence, we open a RO handle.
	db, err := tsdb.OpenDBReadOnly(dbPath, nil)
	if err != nil {
		return mint, maxt, blockTimes, err
	}
	defer db.Close()
	blocks, err := db.Blocks()
	if err != nil {
		return mint, maxt, blockTimes, err
	}
	blockTimes = make([]blockTimestampPair, 0, len(blocks))
	for idx, block := range blocks {
		bmint, bmaxt := block.Meta().MinTime, block.Meta().MaxTime
		blockTimes = append(blockTimes, blockTimestampPair{start: bmint, end: bmaxt})
		if idx == 0 {
			mint, maxt = bmint, bmaxt
		} else {
			mint = value.MinInt64(mint, bmint)
			maxt = value.MaxInt64(maxt, bmaxt)
		}
	}
	return mint, maxt, blockTimes, nil
}
