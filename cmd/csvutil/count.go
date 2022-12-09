package csvutil

import (
	"fmt"

	"github.com/MohamedAbdeen21/csvutil/pkg/csvutil"
	"github.com/spf13/cobra"
)

var possibleModes = []string{"lines", "bytes", "group"}

func existsIn(value string, list []string) bool {
	for _, val := range list {
		if val == value {
			return true
		}
	}
	return false
}

var mode string
var group string
var filters map[string]string

var countCmd = &cobra.Command{
	Use:   "count",
	Short: "Count the number of lines",
	Args:  cobra.RangeArgs(0, 1),
	Run: func(cmd *cobra.Command, args []string) {

		if threads < 1 {
			panic("Threads can't be less than 1")
		}

		if !existsIn(mode, possibleModes) {
			panic("Mode must be one of the possible values")
		}

		if group != "" {
			mode = "group"
		}

		if len(delimiter) != 1 {
			panic("Delimiter must be a single character")
		}

		var count map[string]int64 = make(map[string]int64)
		// use stdin
		if len(args) == 0 {
			println("using Stdin")
			count = csvutil.Count("", 1, mode, filters, group, delimiter)
		} else {
			// pass the name not the fd because we need to os.Stat the file
			count = csvutil.Count(args[0], threads, mode, filters, group, delimiter)
		}

		for key, value := range count {
			fmt.Printf("%s: %d\n", key, value)
		}
	},
}

func init() {
	countCmd.Flags().StringVarP(&mode, "mode", "m", "lines", "What to count\n{lines, bytes}")
	countCmd.Flags().StringVarP(&group, "group", "g", "", "Group by column and return count")
	countCmd.Flags().StringToStringVarP(&filters, "filter", "f", map[string]string{}, "Filter where COLUMN=VALUE")

	countCmd.MarkFlagsMutuallyExclusive("group", "mode")
	countCmd.MarkFlagsMutuallyExclusive("group", "filter")

	rootCmd.AddCommand(countCmd)
}
