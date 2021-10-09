package main

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

const BUFFER int = 1024
const BASEDIR = "/Users/olcayguzel//Desktop/Input2/"

func main() {
	listener, err := net.Listen("tcp", "127.0.0.1:5050")
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	for {
		fmt.Println("Connection waiting")
		connection, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		filename := generateFileName()
		go handleConnection(connection, filename)
	}
}

func generateFileName() string {
	filename := ""
	hash := sha1.New()
	hash.Write([]byte(time.Now().String()))
	result := hash.Sum(nil)
	filename = fmt.Sprintf("%s%x%x.cdr", BASEDIR, os.PathSeparator, result)
	return filename
}

func handleConnection(connection net.Conn, filename string) {
	defer func() {
		fmt.Println("Connection closing...")
		connection.Close()
	}()
	content := make([]string, 0)
	line := make([]string, 0)
	var read int = 1
	for read > 0 {
		buffer := make([]byte, 1)
		read, _ = connection.Read(buffer)
		if read > 0 && buffer[0] != '|' {
			line = append(line, string(buffer[0]))
		} else if read > 0 && buffer[0] == '|' {
			content = append(content, strings.Join(line, ""))
			line = []string{}
		}
	}

	if len(content) > 0 {
		writeToFile(content, filename)
	}
}

func writeToFile(content []string, filename string) {
	file, _ := os.Create(filename)
	defer func() {
		file.Close()
		err := recover()
		if err != nil {
			fmt.Println(err)
		}
	}()
	w := bufio.NewWriter(file)
	for _, line := range content {
		fmt.Fprintln(w, line)
	}
	w.Flush()
	fmt.Printf("File created at: %s", filename)
}
