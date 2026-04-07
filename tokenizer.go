package corkscrewdb

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"unicode/utf8"
)

// Tokenizer encodes and decodes text using Byte-Pair Encoding.
type Tokenizer struct {
	vocab    map[string]int
	inverse  []string
	merges   []mergePair
	special  specialTokens
	hasSpec  bool
	useGlyph bool
}

type mergePair struct {
	a, b string
	rank int
}

type specialTokens struct {
	CLS int
	SEP int
	PAD int
	UNK int
}

type tokenizerFiles struct {
	VocabPath         string
	MergesPath        string
	SpecialTokensPath string
}

func newTokenizer(vocab map[string]int, merges []string) *Tokenizer {
	parsed := make([]mergePair, 0, len(merges))
	for i, merge := range merges {
		parts := strings.SplitN(merge, " ", 2)
		if len(parts) != 2 {
			continue
		}
		parsed = append(parsed, mergePair{a: parts[0], b: parts[1], rank: i})
	}

	maxID := 0
	for _, id := range vocab {
		if id > maxID {
			maxID = id
		}
	}
	inverse := make([]string, maxID+1)
	for tok, id := range vocab {
		if id >= 0 && id < len(inverse) {
			inverse[id] = tok
		}
	}

	spec := specialTokens{}
	hasSpec := false
	if id, ok := vocab["[CLS]"]; ok {
		spec.CLS = id
		hasSpec = true
	}
	if id, ok := vocab["[SEP]"]; ok {
		spec.SEP = id
		hasSpec = true
	}
	if id, ok := vocab["[PAD]"]; ok {
		spec.PAD = id
		hasSpec = true
	}
	if id, ok := vocab["[UNK]"]; ok {
		spec.UNK = id
		hasSpec = true
	}

	useGlyph := false
	for tok := range vocab {
		if strings.ContainsRune(tok, '\u0120') {
			useGlyph = true
			break
		}
	}

	return &Tokenizer{
		vocab:    vocab,
		inverse:  inverse,
		merges:   parsed,
		special:  spec,
		hasSpec:  hasSpec,
		useGlyph: useGlyph,
	}
}

func loadTokenizer(files tokenizerFiles) (*Tokenizer, error) {
	vocab, err := loadVocab(files.VocabPath)
	if err != nil {
		return nil, fmt.Errorf("corkscrewdb: load vocab: %w", err)
	}
	merges, err := loadMerges(files.MergesPath)
	if err != nil {
		return nil, fmt.Errorf("corkscrewdb: load merges: %w", err)
	}
	special, err := loadSpecialTokens(files.SpecialTokensPath)
	if err != nil {
		return nil, fmt.Errorf("corkscrewdb: load special tokens: %w", err)
	}

	maxID := 0
	for _, id := range vocab {
		if id > maxID {
			maxID = id
		}
	}
	inverse := make([]string, maxID+1)
	for tok, id := range vocab {
		if id >= 0 && id < len(inverse) {
			inverse[id] = tok
		}
	}

	useGlyph := false
	for tok := range vocab {
		if strings.ContainsRune(tok, '\u0120') {
			useGlyph = true
			break
		}
	}

	return &Tokenizer{
		vocab:    vocab,
		inverse:  inverse,
		merges:   merges,
		special:  special,
		hasSpec:  true,
		useGlyph: useGlyph,
	}, nil
}

func (t *Tokenizer) Encode(text string) []int {
	if t.hasSpec {
		return t.encodeWithSpecial(text)
	}
	return t.encodeRaw(text)
}

func (t *Tokenizer) encodeWithSpecial(text string) []int {
	if text == "" {
		return []int{t.special.CLS, t.special.SEP}
	}
	words := t.preTokenizeText(text)
	ids := make([]int, 0, len(words)+2)
	ids = append(ids, t.special.CLS)
	for _, word := range words {
		ids = append(ids, t.encodeWord(word)...)
	}
	ids = append(ids, t.special.SEP)
	return ids
}

func (t *Tokenizer) encodeRaw(text string) []int {
	if text == "" {
		return nil
	}
	words := t.preTokenizeText(text)
	var ids []int
	for _, word := range words {
		ids = append(ids, t.encodeWord(word)...)
	}
	return ids
}

func (t *Tokenizer) preTokenizeText(text string) []string {
	if t.useGlyph {
		return preTokenize(text)
	}
	return preTokenizeSimple(text)
}

func (t *Tokenizer) Decode(ids []int) string {
	var sb strings.Builder
	for _, id := range ids {
		if t.hasSpec && (id == t.special.CLS || id == t.special.SEP || id == t.special.PAD) {
			continue
		}
		if id >= 0 && id < len(t.inverse) {
			tok := strings.ReplaceAll(t.inverse[id], "\u0120", " ")
			sb.WriteString(tok)
		}
	}
	return sb.String()
}

func (t *Tokenizer) VocabSize() int {
	return len(t.vocab)
}

func (t *Tokenizer) encodeWord(word string) []int {
	symbols := make([]string, 0, utf8.RuneCountInString(word))
	for _, r := range word {
		symbols = append(symbols, string(r))
	}

	mergeRank := make(map[string]int, len(t.merges))
	for i, merge := range t.merges {
		mergeRank[merge.a+" "+merge.b] = i
	}

	for len(symbols) > 1 {
		bestRank := len(t.merges)
		bestIdx := -1
		for i := 0; i < len(symbols)-1; i++ {
			key := symbols[i] + " " + symbols[i+1]
			if rank, ok := mergeRank[key]; ok && rank < bestRank {
				bestRank = rank
				bestIdx = i
			}
		}
		if bestIdx < 0 {
			break
		}
		merged := symbols[bestIdx] + symbols[bestIdx+1]
		next := make([]string, 0, len(symbols)-1)
		next = append(next, symbols[:bestIdx]...)
		next = append(next, merged)
		next = append(next, symbols[bestIdx+2:]...)
		symbols = next
	}

	ids := make([]int, len(symbols))
	for i, sym := range symbols {
		if id, ok := t.vocab[sym]; ok {
			ids[i] = id
		} else if t.hasSpec {
			ids[i] = t.special.UNK
		} else {
			ids[i] = -1
		}
	}

	if !t.hasSpec {
		filtered := ids[:0]
		for _, id := range ids {
			if id >= 0 {
				filtered = append(filtered, id)
			}
		}
		return filtered
	}
	return ids
}

func preTokenizeSimple(text string) []string {
	var words []string
	var current strings.Builder
	for _, r := range text {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			if current.Len() > 0 {
				words = append(words, current.String())
				current.Reset()
			}
			words = append(words, string(r))
			continue
		}
		if isPunctuation(r) {
			if current.Len() > 0 {
				words = append(words, current.String())
				current.Reset()
			}
			words = append(words, string(r))
			continue
		}
		current.WriteRune(r)
	}
	if current.Len() > 0 {
		words = append(words, current.String())
	}
	return words
}

func preTokenize(text string) []string {
	var words []string
	var current strings.Builder
	prevSpace := true
	for _, r := range text {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			if current.Len() > 0 {
				words = append(words, current.String())
				current.Reset()
			}
			prevSpace = true
			continue
		}
		if prevSpace && len(words) > 0 {
			current.WriteRune('\u0120')
		}
		prevSpace = false
		if isPunctuation(r) {
			if current.Len() > 0 {
				words = append(words, current.String())
				current.Reset()
			}
			words = append(words, string(r))
			continue
		}
		current.WriteRune(r)
	}
	if current.Len() > 0 {
		words = append(words, current.String())
	}
	return words
}

func isPunctuation(r rune) bool {
	switch r {
	case '.', ',', '!', '?', ';', ':', '"', '\'', '(', ')', '[', ']', '{', '}',
		'-', '/', '\\', '@', '#', '$', '%', '^', '&', '*', '+', '=', '<', '>', '|', '~', '`':
		return true
	}
	return false
}

func loadVocab(path string) (map[string]int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var vocab map[string]int
	if err := json.Unmarshal(data, &vocab); err != nil {
		return nil, err
	}
	return vocab, nil
}

func loadMerges(path string) ([]mergePair, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var merges []mergePair
	scanner := bufio.NewScanner(f)
	rank := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			continue
		}
		merges = append(merges, mergePair{a: parts[0], b: parts[1], rank: rank})
		rank++
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return merges, nil
}

func loadSpecialTokens(path string) (specialTokens, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return specialTokens{}, err
	}
	var raw map[string]int
	if err := json.Unmarshal(data, &raw); err != nil {
		return specialTokens{}, err
	}
	return specialTokens{
		CLS: raw["[CLS]"],
		SEP: raw["[SEP]"],
		PAD: raw["[PAD]"],
		UNK: raw["[UNK]"],
	}, nil
}
