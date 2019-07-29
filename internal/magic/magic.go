// package magic for magic numbers
package magic

const (
	Index   = 0x1000
	Data    = 0x2000
	Bigdata = 0x6000
	Bucket  = 0x5000
	Head    = 0x9000
)

const (
	FileIdent string = "bardbv01"
)

const (
	TypeMask uint16 = 0xF000
)

var AddressMask = [33]uint32{
	0x00000000, //  0
	0x80000000, //  1
	0xc0000000, //  2
	0xe0000000, //  3
	0xf0000000, //  4
	0xf8000000, //  5
	0xfc000000, //  6
	0xfe000000, //  7
	0xff000000, //  8
	0xff800000, //  9
	0xffc00000, // 10
	0xffe00000, // 11
	0xfff00000, // 12
	0xfff80000, // 13
	0xfffc0000, // 14
	0xfffe0000, // 15
	0xffff0000, // 16
	0xffff8000, // 17
	0xffffc000, // 18
	0xffffe000, // 19
	0xfffff000, // 20
	0xfffff800, // 21
	0xfffffc00, // 22
	0xfffffe00, // 23
	0xffffff00, // 24
	0xffffff80, // 25
	0xffffffc0, // 26
	0xffffffe0, // 27
	0xfffffff0, // 28
	0xfffffff8, // 29
	0xfffffffc, // 30
	0xfffffffe, // 31
	0xffffffff, // 32
}

var PlaceBased = [32]uint32{
	0x80000000, //  0
	0x40000000, //  1
	0x20000000, //  2
	0x10000000, //  3
	0x08000000, //  4
	0x04000000, //  5
	0x02000000, //  6
	0x01000000, //  7
	0x00800000, //  8
	0x00400000, //  9
	0x00200000, // 10
	0x00100000, // 11
	0x00080000, // 12
	0x00040000, // 13
	0x00020000, // 14
	0x00010000, // 15
	0x00008000, // 16
	0x00004000, // 17
	0x00002000, // 18
	0x00001000, // 19
	0x00000800, // 20
	0x00000400, // 21
	0x00000200, // 22
	0x00000100, // 23
	0x00000080, // 24
	0x00000040, // 25
	0x00000020, // 26
	0x00000010, // 27
	0x00000008, // 28
	0x00000004, // 29
	0x00000002, // 30
	0x00000001, // 31
}
