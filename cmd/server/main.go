package main

import (
	"fmt"
	"os"
	"os/exec"
	"sync"

	log "github.com/sirupsen/logrus"
)

func main() {
	command := "go"
	//
	wg := sync.WaitGroup{}
	log.Info("starting")

	for i := range 5 {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("running ", i)

			idx := fmt.Sprintf("%d", i)
            port := fmt.Sprintf(":808%d", i)
			args := []string{"run", "main.go", "db.go", "schema.go", "--self-id", idx, "-a", port}
			// args := []string{"--self-id", idx}
			cmd := exec.Command(command, args...)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr

			// stdout, err := cmd.Output()
			err := cmd.Run()

			if err != nil {
				log.Error(err.Error())
				return
			}

			// Print the output

		}()
	}

	wg.Wait()
}
