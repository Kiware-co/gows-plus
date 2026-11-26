package media

import (
	"os"
	"testing"
)

func TestParseWebP(t *testing.T) {
	cases := []struct {
		file     string
		animated bool
	}{
		{"testdata/anim.webp", true},
		{"testdata/quieto.webp", false},
	}
	for _, c := range cases {
		data, err := os.ReadFile(c.file)
		if err != nil {
			t.Fatal(err)
		}
		info, err := ParseWebP(data)
		if err != nil {
			t.Fatalf("%s: %v", c.file, err)
		}
		if info.Animated != c.animated {
			t.Errorf("%s: animated = %v, want %v", c.file, info.Animated, c.animated)
		}
		if info.Width != 512 || info.Height != 512 {
			t.Errorf("%s: size = %dx%d, want 512x512", c.file, info.Width, info.Height)
		}
	}
}

func TestParseWebPRejectsOtherFormats(t *testing.T) {
	png := []byte("\x89PNG\r\n\x1a\n0000000000000000000000000000")
	if _, err := ParseWebP(png); err == nil {
		t.Fatal("expected error for PNG")
	}
}
