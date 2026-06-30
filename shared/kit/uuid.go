package kit

import (
	"crypto/rand"
	"fmt"
)

// NewShortUUID 无横线，32位小写字符串，用作event_id最合适
func NewShortUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x", b)
}
