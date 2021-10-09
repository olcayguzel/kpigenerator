package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"time"
)

var count = 0

var now time.Time = time.Now()
var max time.Time = time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, now.Location())

func main() {
	timer := time.Tick(time.Minute * 1)
	timertick()
	for {
		<-timer
		timertick()
	}

}

func timertick() {
	s := count + 1
	min := max.Add(time.Minute * time.Duration(-1))
	content := generateCDRData(min, max)
	writeToFile(content, min, max, s)
	max = max.Add(time.Minute * time.Duration(1))
}

func generateCDRData(start time.Time, end time.Time) []string {
	causecodes:= []int {110, 503, 302}
	template := `%d,2021-10-01 %02d:%02d:%02d,123412121234567,905324105295,,172.16.150.110,3,4,,,,1169-27583@172.16.150.110,3,12121234567,3,12121234567,4,12121234567,4,12121234567,0,,0,,0,,0,,0,,0,,1,4,%d,%d`
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
			
			content := fmt.Sprintf(template, count, hour, minute, start.Second(), causecodes[rnd.Intn(2)], rnd.Intn(500))
			contents = append(contents, content)
		}
		start = start.Add(time.Minute)
	}
	return contents
}

func writeToFile(content []string, start time.Time, end time.Time, rangestart int) {
	path := fmt.Sprintf("C:\\Odine\\Input\\cdr_%02d%02d-%02d%02d_%d-%d.cdr", start.Hour(), start.Minute(), end.Hour(), end.Minute(), rangestart, count)
	file, _ := os.Create(path)
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
	fmt.Printf("File created at: %s\n", path)
}
