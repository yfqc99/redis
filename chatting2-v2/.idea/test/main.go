package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func main() {
	str := "hello中国"
	len := int32(len(str))
	fmt.Println("字符串的字节数：", len)
	var pkg = new(bytes.Buffer)
	binary.Write(pkg, binary.LittleEndian, len)
	fmt.Printf("pkg的类型：%T\n", pkg)
	fmt.Println("byte切片的值：", pkg.Bytes())
	var len1 int32
	binary.Read(pkg, binary.LittleEndian, &len1)
	fmt.Println("字符串的字节数：", len1)
	/*字符串的字节数： 11
	pkg的类型：*bytes.Buffer
	byte切片的值： [11 0 0 0]
	字符串的字节数： 11*/
}
