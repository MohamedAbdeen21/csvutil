package csvutil

import (
	"fmt"
	"os"

	"github.com/MohamedAbdeen21/csvutil/pkg/csvutil"
	"github.com/spf13/cobra"
)

var mode string
var group string
var count_filters map[string]string

var countCmd = &cobra.Command{
	Use:   "count",
	Short: "Count the number of lines, bytes or frequency of column values",
	Args:  cobra.RangeArgs(0, 1),
	Run: func(cmd *cobra.Command, args []string) {

		if !csvutil.ExistsIn(mode, countPossibleModes) {
			cmd.PrintErrln("Mode must be one of the possible values")
			return
		}

		if group != "" {
			mode = "group"
		}

		var count map[string]int64 = make(map[string]int64)
		var err error
		// use stdin
		if len(args) == 0 {
			count, err = csvutil.Count(os.Stdin.Name(), 1, mode, count_filters, group, delimiter)
		} else {
			count, err = csvutil.Count(args[0], threads, mode, count_filters, group, delimiter)
		}

		if err != nil {
			cmd.PrintErrln(err)
			os.Exit(1)
		}

		for key, value := range count {
			cmd.Printf("%s: %d\n", key, value)
		}
	},
}

func init() {

	possibleModesString := csvutil.ConstructStringFromList(countPossibleModes)

	countCmd.Flags().StringVarP(&mode, "mode", "m", "lines", fmt.Sprintf("What to count\n%s", possibleModesString))
	countCmd.Flags().StringVarP(&group, "group", "g", "", "Group by column and return count")
	countCmd.Flags().StringToStringVarP(&count_filters, "filter", "f", map[string]string{}, "Filter where COLUMN=VALUE")

	countCmd.MarkFlagsMutuallyExclusive("group", "mode")

	rootCmd.AddCommand(countCmd)
}
