package main

import "os"

func main() {
	metric := []byte("lvl=info msg= count#count=1 foo=\"bar\" size=10\n")

	for {
		_, _ = os.Stdout.Write(metric)
	}
}
