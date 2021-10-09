package main

import (
	"fmt"
	"math/rand"
	"net"
	"time"
)

var count = 0
var now time.Time = time.Now()
var max time.Time = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, now.Location())
var BUFFER = 1024

func main() {
	timer := time.Tick(time.Minute * 1)
	timertick()
	for {
		<-timer
		timertick()
	}

}

func timertick() {
	min := max.Add(time.Minute * time.Duration(-1))
	content := generateCDRData(min, max)
	sendto(content)
	//writeToFile(content, min, max, s)
	max = max.Add(time.Minute * time.Duration(1))
}

func generateCDRData(start time.Time, end time.Time) []string {
	causecodes:= []int {404, 503, 302}
	template := `%d,%04d-%02d-%02d %02d:%02d:%02d,123412121234567,905324105295,,172.16.150.110,3,4,,,,1169-27583@172.16.150.110,3,12121234567,3,12121234567,4,12121234567,4,12121234567,0,,0,,0,,0,,0,,0,,1,4,%d,%d`
	contents := []string{}
	rnd := rand.New(rand.NewSource(time.Now().UnixMicro()))
	for {
		if start.Sub(end).Seconds() > 0 {
			break
		}
		quantity := rnd.Intn(100)
		for i := 0; i < quantity; i++ {
			if rnd.Intn(10)%3 == 0 {
				start = start.Add(time.Second)
				continue
			}
			count++
			hour := start.Hour()
			minute := start.Minute()
			if start.Second() >= 0 && start.Second() <= 10 {
				oldcdr := rnd.Intn(100)
				if oldcdr > 0 && (oldcdr % 10) == 0 {
					pad := rnd.Intn(2)
					if minute - pad >= 0 {
						minute-= pad
					} else {
						minute = 60 - pad
						hour--
					}
				}
			}

			content := fmt.Sprintf(template, count, start.Year(), start.Month(), start.Day(), hour, minute, start.Second(), causecodes[rnd.Intn(2)], rnd.Intn(500))
			contents = append(contents, content)
		}
		start = start.Add(time.Minute)
	}
	return contents
}

func sendto(content []string) {
	client, err := net.Dial("tcp", "127.0.0.1:5050")
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, line := range content {
		fmt.Println(line)
		client.Write([]byte(line))
	}
	fmt.Println("file sent")
	client.Close()
}
