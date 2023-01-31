package main

import (
	"os"

	csvutil "github.com/MohamedAbdeen21/csvutil/cmd"
)

func main() {
	if err := csvutil.RootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
