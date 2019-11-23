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
	"encoding/binary"
	"encoding/gob"
	"github.com/go-kit/kit/log"
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
	"sort"
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
	dirs       []string
}

// Content Type for the Open Metrics Parser.
// Needed to init the text parser.
const contentType = "application/openmetrics-text; version=0.0.1; charset=utf-8"

// ImportFromFile imports data from a file formatted according to the Open Metrics format,
// converts it into block(s), and places the newly created block(s) in the
// TSDB DB directory, where it is treated like any other block.
func ImportFromFile(filePath string, dbPath string, maxSamplesInMemory int, logger log.Logger) error {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	tmpDbDir, err := ioutil.TempDir("", "importer")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDbDir)

	dbMint, dbMaxt, blockTimes, err := getDbTimes(dbPath)
	if err != nil {
		return err
	}

	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	sampleIndexes, blockMetas, err := collectSampleInformation(f, dbMint, dbMaxt, blockTimes, logger)
	if err != nil {
		return err
	}

	// Set reader to starting point to allow another pass.
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	expandedBlockMetas := expandBlockMetas(blockMetas)

	err = writeSamples(f, tmpDbDir, sampleIndexes, expandedBlockMetas, maxSamplesInMemory, logger)
	if err != nil {
		return err
	}

	// Compact blocks created by us, that share the same time range.
	for _, metas := range expandedBlockMetas {
		for _, meta := range metas {
			if len(meta.dirs) > 1 {
				compactedPath, err := compactBlocks(tmpDbDir, meta.dirs, logger)
				if err != nil {
					return err
				}
				// Delete blocks after compaction.
				for _, dir := range meta.dirs {
					err = os.RemoveAll(dir)
					if err != nil {
						return err
					}
				}
				meta.dirs = []string{compactedPath}
			}
		}
	}

	err = fileutil.CopyDirs(tmpDbDir, dbPath)
	if err != nil {
		return err
	}

	return nil
}

// collectSampleInformation reads every sample, assigns each of them to a block, and collects per-block
// information required for the actual write stage.
func collectSampleInformation(f *os.File, dbMint, dbMaxt int64, blockTimes []blockTimestampPair, logger log.Logger) ([]int, map[int]*newBlockMeta, error) {
	blockIndexes := make([]int, 0)
	blockMetas := make(map[int]*newBlockMeta)

	// We have at least 2 block metas - before dbMint, and after dbMaxt.
	blockMetas[0] = &newBlockMeta{index: 0, count: 0, mint: math.MaxInt64, maxt: dbMint, isAligned: false}
	for idx, block := range blockTimes {
		blockMetas[idx+1] = &newBlockMeta{index: idx + 1, count: 0, mint: block.start, maxt: block.end, isAligned: true}
	}
	blockMetas[len(blockTimes)+1] = &newBlockMeta{index: len(blockTimes) + 1, count: 0, mint: dbMaxt, maxt: math.MinInt64, isAligned: false}

	// Fetch timestamps only.
	streamTimes := func(data []byte, atEOF bool) (int, []byte, error) {
		var err error
		advance := 0
		lineIndex := 0
		lines := strings.Split(string(data), "\n")
		parser := textparse.New(data, contentType)
		for {
			var entry textparse.Entry
			if entry, err = parser.Next(); err != nil {
				if !atEOF && entry == textparse.EntryInvalid {
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
			if entry != textparse.EntrySeries {
				advance = advance + lineLength
				continue
			}
			_, ctime, _ := parser.Series()
			if ctime == nil {
				return 0, nil, NoTimestampError
			} else {
				// OpenMetrics parser multiples times by 1000 - undoing that.
				ctimeCorrected := *ctime / 1000
				ctimeBuf := make([]byte, binary.MaxVarintLen64)
				n := binary.PutVarint(ctimeBuf, ctimeCorrected)
				advance += lineLength
				return advance, ctimeBuf[:n], nil
			}
		}
	}

	// Use a streaming approach to avoid loading too much data at once.
	scanner := bufio.NewScanner(f)
	scanner.Split(streamTimes)
	for scanner.Scan() {
		ctimeBytes := scanner.Bytes()
		ctime, _ := binary.Varint(ctimeBytes)

		var sampleBlockIndex int
		if ctime < dbMint || len(blockTimes) == 0 {
			sampleBlockIndex = 0
		} else if ctime >= dbMaxt {
			sampleBlockIndex = len(blockTimes) + 1
		} else {
			sampleBlockIndex = getBlockIndex(ctime, blockTimes)
			if sampleBlockIndex == -1 {
				return blockIndexes, blockMetas, NoBlockFoundError
			}
			// Adjusting for block before dbMint.
			sampleBlockIndex = sampleBlockIndex + 1
		}
		meta, ok := blockMetas[sampleBlockIndex]
		if !ok {
			return blockIndexes, blockMetas, NoBlockMetaFoundError
		}
		meta.mint = value.MinInt64(meta.mint, ctime)
		meta.maxt = value.MaxInt64(meta.maxt, ctime)
		meta.count += 1
		blockIndexes = append(blockIndexes, sampleBlockIndex)
	}

	if err := scanner.Err(); err != nil {
		return blockIndexes, blockMetas, err
	}

	return blockIndexes, blockMetas, nil
}

// writeSamples parses each metric sample, and writes the samples to the correspondingly aligned block.
// It uses the block indexes assigned to each sample, and block meta info, gathered in collectSampleInformation().
func writeSamples(f *os.File, dbDir string, indexes []int, metas map[int][]*newBlockMeta, maxSamplesInMemory int, logger log.Logger) error {
	blocks := getEmptyBlocks(len(metas))
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
					TimestampMs: ctimeCorrected,
					Value:       cvalue,
					Labels:      labels.FromMap(clabels.Map()),
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
		// TODO (dipack95): Check to see if we can use something like sync.Pool to reuse mem.
		if currentPassCount == 0 {
			blocks = getEmptyBlocks(len(metas))
		}

		encSample := scanner.Bytes()
		decBuf := bytes.NewBuffer(encSample)
		sample := tsdb.MetricSample{}
		err := gob.NewDecoder(decBuf).Decode(&sample)
		if err != nil {
			return err
		}

		blockIndex := indexes[sampleCount]
		blocks[blockIndex] = append(blocks[blockIndex], &sample)

		currentPassCount += 1
		sampleCount += 1
		// Have enough samples to write to disk.
		if currentPassCount == maxSamplesInMemory || sampleCount == len(indexes) {
			for blockIdx, block := range blocks {
				// If current block is empty, nothing to write.
				if len(block) == 0 {
					continue
				}
				// Sort input data.
				sort.Slice(block, func(x, y int) bool {
					return block[x].TimestampMs < block[y].TimestampMs
				})
				// Put each sample into the appropriate block.
				bins := binSamples(block, metas[blockIdx])
				for midx, meta := range metas[blockIdx] {
					bmint, bmaxt := meta.mint, meta.maxt
					binned := bins[midx]
					if len(binned) == 0 {
						continue
					}
					path, err := tsdb.CreateBlock(binned, dbDir, bmint, bmaxt, logger)
					if err != nil {
						return err
					}
					meta.dirs = append(meta.dirs, path)
				}
			}
			// Reset current pass count.
			currentPassCount = 0
		}
	}

	if err := scanner.Err(); err != nil {
		return err
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
		pair := blockTimestampPair{start: s}
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

// getBlockIndex returns the index of the block that the timestamp belongs to.
func getBlockIndex(t int64, blockTimes []blockTimestampPair) int {
	for idx, block := range blockTimes {
		if block.start <= t && t < block.end {
			return idx
		}
	}
	return -1
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

// binSamples puts each sample into its corresponding block specified by the block metadatas.
func binSamples(samples []*tsdb.MetricSample, metas []*newBlockMeta) [][]*tsdb.MetricSample {
	bins := getEmptyBlocks(len(metas))
	for _, sample := range samples {
		idx := -1
		for midx, m := range metas {
			if idx != -1 {
				continue
			}
			if m.mint <= sample.TimestampMs && sample.TimestampMs < m.maxt {
				idx = midx
			}
		}
		bins[idx] = append(bins[idx], sample)
	}
	return bins
}

// expandBlockMetas creates a list of block metas for each block we've found so far.
// This includes dividing blocks outside of DB time limits into 2h blocks.
func expandBlockMetas(blockMetas map[int]*newBlockMeta) map[int][]*newBlockMeta {
	expandedBlockMetas := make(map[int][]*newBlockMeta)
	for blockIdx, meta := range blockMetas {
		bmetas := make([]*newBlockMeta, 0)
		if blockIdx == 0 || blockIdx == len(blockMetas)-1 {
			// Divvy blocks outside of DB time limits into 2h blocks.
			twoHourBlocks := makeRange(meta.mint, meta.maxt, tsdb.DefaultBlockDuration)
			for _, block := range twoHourBlocks {
				m := &newBlockMeta{
					index:     blockIdx,
					count:     0,
					mint:      block.start,
					maxt:      block.end,
					isAligned: false,
					dirs:      nil,
				}
				bmetas = append(bmetas, m)
			}
		} else {
			// Blocks that need to be aligned are kept as is.
			bmetas = append(bmetas, meta)
		}
		expandedBlockMetas[blockIdx] = bmetas
	}
	return expandedBlockMetas
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
