package corkscrewdb

import "testing"

func TestTokenizerEncodeDecodeRoundTrip(t *testing.T) {
	vocab := map[string]int{
		"h": 0, "e": 1, "l": 2, "o": 3, " ": 4, "w": 5, "r": 6, "d": 7,
		"he": 8, "ll": 9, "wo": 10, "rld": 11,
		"[CLS]": 100, "[SEP]": 101, "[PAD]": 102, "[UNK]": 103,
	}
	merges := []string{"h e", "l l", "w o", "r l", "rl d"}
	tok := newTokenizer(vocab, merges)
	ids := tok.Encode("hello world")
	text := tok.Decode(ids)
	if text != "hello world" {
		t.Fatalf("Decode(Encode(%q)) = %q", "hello world", text)
	}
}

func TestTokenizerVocabSize(t *testing.T) {
	vocab := map[string]int{"a": 0, "b": 1}
	tok := newTokenizer(vocab, nil)
	if tok.VocabSize() != 2 {
		t.Fatalf("VocabSize = %d, want 2", tok.VocabSize())
	}
}

func TestTokenizerUnknownToken(t *testing.T) {
	vocab := map[string]int{"a": 0, "[CLS]": 1, "[SEP]": 2, "[UNK]": 3}
	tok := newTokenizer(vocab, nil)
	ids := tok.Encode("z")
	want := []int{1, 3, 2}
	if len(ids) != len(want) {
		t.Fatalf("got %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("token[%d] = %d, want %d", i, ids[i], want[i])
		}
	}
}
