package utils

import (
	"encoding/binary"
	"math/bits"
	"time"
	"sort"
)

const (
	k0 = uint64(0xc3a5c85c97cb3127)
	k1 = uint64(0xb492b66fbe98f273)
	k2 = uint64(0x9ae16a3b2f90404f)
)

const (
	c1 = uint32(0xcc9e2d51)
	c2 = uint32(0x1b873593)
	c3 = uint64(0x9ddfea08eb382d69)
)

func ror64(val, shift uint64) uint64 {
	if shift != 0 {
		return val>>shift | val<<(64-shift)
	}
	return val
}

func hash128to64(lo, hi uint64) uint64 {
	a := (lo ^ hi) * c3
	a ^= (a >> 47)
	b := (hi ^ a) * c3
	b ^= (b >> 47)
	b *= c3
	return b
}

func shiftMix(val uint64) uint64 { return val ^ (val >> 47) }
func hash64Len16(u, v uint64) uint64 { return hash128to64(u, v) }

func hash64Len16Mul(u, v, mul uint64) uint64 {
	a := (u ^ v) * mul
	a ^= (a >> 47)
	b := (v ^ a) * mul
	b ^= (b >> 47)
	b *= mul
	return b
}

func hash64Len0to16(s []byte) uint64 {
	n := uint64(len(s))
	if n >= 8 {
		mul := k2 + n*2
		a := binary.LittleEndian.Uint64(s) + k2
		b := binary.LittleEndian.Uint64(s[n-8:])
		c := ror64(b, 37)*mul + a
		d := (ror64(a, 25) + b) * mul
		return hash64Len16Mul(c, d, mul)
	}
	if n >= 4 {
		mul := k2 + n*2
		a := uint64(binary.LittleEndian.Uint32(s))
		return hash64Len16Mul(n+(a<<3), uint64(binary.LittleEndian.Uint32(s[n-4:])), mul)
	}
	if n > 0 {
		a := s[0]
		b := s[n>>1]
		c := s[n-1]
		y := uint32(a) + uint32(b)<<8
		z := uint32(n) + uint32(c)<<2
		return shiftMix(uint64(y)*k2^uint64(z)*k0) * k2
	}
	return k2
}

func hash64Len17to32(s []byte) uint64 {
	n := uint64(len(s))
	mul := k2 + n*2
	a := binary.LittleEndian.Uint64(s) * k1
	b := binary.LittleEndian.Uint64(s[8:])
	c := binary.LittleEndian.Uint64(s[n-8:]) * mul
	d := binary.LittleEndian.Uint64(s[n-16:]) * k2
	return hash64Len16Mul(ror64(a+b, 43)+ror64(c, 30)+d, a+ror64(b+k2, 18)+c, mul)
}

func bswap64(in uint64) uint64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], in)
	return binary.BigEndian.Uint64(buf[:])
}

func hash64Len33to64(s []byte) uint64 {
	n := uint64(len(s))
	mul := k2 + n*2
	a := binary.LittleEndian.Uint64(s) * k2
	b := binary.LittleEndian.Uint64(s[8:])
	c := binary.LittleEndian.Uint64(s[n-24:])
	d := binary.LittleEndian.Uint64(s[n-32:])
	e := binary.LittleEndian.Uint64(s[16:]) * k2
	f := binary.LittleEndian.Uint64(s[24:]) * 9
	g := binary.LittleEndian.Uint64(s[n-8:])
	h := binary.LittleEndian.Uint64(s[n-16:]) * mul
	u := ror64(a+g, 43) + (ror64(b, 30)+c)*9
	v := ((a + g) ^ d) + f + 1
	w := bswap64((u+v)*mul) + h
	x := ror64(e+f, 42) + c
	y := (bswap64((v+w)*mul) + g) * mul
	z := e + f + c
	a = bswap64((x+z)*mul+y) + b
	b = shiftMix((z+a)*mul+d+h) * mul
	return b + x
}

func weakHashLen32WithSeeds(s []byte, a, b uint64) (uint64, uint64) {
	// Note: Was two overloads of WeakHashLen32WithSeeds.  The second is only
	// ever called from the first, so I inlined it.
	w := binary.LittleEndian.Uint64(s)
	x := binary.LittleEndian.Uint64(s[8:])
	y := binary.LittleEndian.Uint64(s[16:])
	z := binary.LittleEndian.Uint64(s[24:])

	a += w
	b = ror64(b+a+z, 21)
	c := a
	a += x
	a += y
	b += ror64(a, 44)
	return a + z, b + c
}


// Hash64 returns a 64-bit hash for a slice of bytes.
func Hash64(s []byte) uint64 {
	n := uint64(len(s))
	if n <= 32 {
		if n <= 16 {
			return hash64Len0to16(s)
		}
		return hash64Len17to32(s)
	} else if n <= 64 {
		return hash64Len33to64(s)
	}

	// For strings over 64 bytes we hash the end first, and then as we loop we
	// keep 56 bytes of state: v, w, x, y, and z.
	x := binary.LittleEndian.Uint64(s[n-40:])
	y := binary.LittleEndian.Uint64(s[n-16:]) + binary.LittleEndian.Uint64(s[n-56:])
	z := hash64Len16(binary.LittleEndian.Uint64(s[n-48:])+n, binary.LittleEndian.Uint64(s[n-24:]))

	v1, v2 := weakHashLen32WithSeeds(s[n-64:], n, z)
	w1, w2 := weakHashLen32WithSeeds(s[n-32:], y+k1, x)
	x = x*k1 + binary.LittleEndian.Uint64(s)

	// Decrease n to the nearest multiple of 64, and operate on 64-byte chunks.
	n = (n - 1) &^ 63
	for {
		x = ror64(x+y+v1+binary.LittleEndian.Uint64(s[8:]), 37) * k1
		y = ror64(y+v2+binary.LittleEndian.Uint64(s[48:]), 42) * k1
		x ^= w2
		y += v1 + binary.LittleEndian.Uint64(s[40:])
		z = ror64(z+w1, 33) * k1
		v1, v2 = weakHashLen32WithSeeds(s, v2*k1, x+w1)
		w1, w2 = weakHashLen32WithSeeds(s[32:], z+w2, y+binary.LittleEndian.Uint64(s[16:]))
		z, x = x, z
		s = s[64:]
		n -= 64
		if n == 0 {
			break
		}
	}
	return hash64Len16(hash64Len16(v1, w1)+shiftMix(y)*k1+z, hash64Len16(v2, w2)+x)
}

func HashUint32(v...uint32) uint64 {
	bs := make([]byte, 4*len(v))
	for i, d := range v {
		binary.BigEndian.PutUint32(bs[i*4:], d)
	}
	return Hash64(bs)
}

func appendSort(data []uint32, el uint32) []uint32 {
	index := sort.Search(len(data), func(i int) bool { return data[i] > el })
	data = append(data, 0)
	copy(data[index+1:], data[index:])
	data[index] = el
	return data
}

func getSortedKeysFromMapAndLen(iData map[uint32]uint32) ([]uint32, int) {
	count := len(iData)
	keys := make([]uint32, 0, count)
	for k := range iData {
		keys = appendSort(keys, k)
	}
	return keys, count
}

func getSortedKeysAndLen(iData map[uint32]struct{}) ([]uint32, int) {
	count := len(iData)
	keys := make([]uint32, 0, count)
	for k := range iData {
		keys = appendSort(keys, k)
	}
	return keys, count
}

func HashMapUint32(iData map[uint32]uint32) (uint64, []uint32) {
	keys, count := getSortedKeysFromMapAndLen(iData)
	bs := make([]byte, 4*count*2)
	for i, k := range keys {
		binary.BigEndian.PutUint32(bs[i*4:], k)
		binary.BigEndian.PutUint32(bs[(i*4)+count:], iData[k])
	}
	return Hash64(bs), keys
}

func HashKeysUint32(iData map[uint32]struct{}) (uint64, []uint32) {
	keys, count := getSortedKeysAndLen(iData)
	bs := make([]byte, 4*count)
	for i, k := range keys {
		binary.BigEndian.PutUint32(bs[i*4:], k)
	}
	return Hash64(bs), keys
}

type Uint32Slice []uint32
func (p Uint32Slice) Len() int           { return len(p) }
func (p Uint32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func HashSliceUint32(keys []uint32) (uint64, []uint32) {
	sort.Sort(Uint32Slice(keys))
	bs := make([]byte, 4*len(keys))
	for i, k := range keys {
		binary.BigEndian.PutUint32(bs[i*4:], k)
	}
	return Hash64(bs), keys
}

func Range(v uint64) int {
	return 63-bits.LeadingZeros64(v)
}

const (
	DaySeconds = 24 * 60 * 60
)

func GetDaySlot(t time.Time) int64 {
	ts := t.Unix()
	return (ts - (ts % DaySeconds))/DaySeconds
}

func GetSlots(t time.Time) (int64, int, int) {
	ts := t.Unix()
	s := (ts % DaySeconds)
	return ts - s, int(s/60), int(s)
}
