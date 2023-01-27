package csvutil

import (
	"fmt"
	"os"

	csvutil "github.com/MohamedAbdeen21/csvutil/pkg"
	"github.com/spf13/cobra"
)

var plotCountCmd = &cobra.Command{
	Use:   "count",
	Short: "Count the number of lines, bytes or frequency of column",
	Args:  cobra.RangeArgs(0, 1),
	Run: func(cmd *cobra.Command, args []string) {

		if !csvutil.ExistsIn(mode, countPossibleModes) {
			panic("Mode must be one of the possible values")
		}

		if group != "" {
			mode = "group"
		}

		var count map[string]int64 = make(map[string]int64)
		var err error
		// use stdin
		if len(args) == 0 {
			cmd.Print(">")
			count, err = csvutil.Count(&csvutil.Options{
				Filename:  os.Stdin.Name(),
				Threads:   1,
				Mode:      mode,
				Filters:   count_filters,
				Group:     group,
				Delimiter: delimiter,
			})
		} else {
			count, err = csvutil.Count(&csvutil.Options{
				Filename:  args[0],
				Threads:   threads,
				Mode:      mode,
				Filters:   count_filters,
				Group:     group,
				Delimiter: delimiter,
			})
		}

		if err != nil {
			cmd.PrintErrln(err)
			os.Exit(1)
		}

		for key, value := range count {
			if key == "NULL" {
				continue
			}
			cmd.Printf("%s: %d\n", key, value)
		}
	},
}

func init() {
	possibleModesString := csvutil.ConstructStringFromList(countPossibleModes)

	plotCountCmd.Flags().StringVarP(&mode, "mode", "m", "lines", fmt.Sprintf("What to count\n%s", possibleModesString))
	plotCountCmd.Flags().StringVarP(&group, "group", "g", "", "Group by column and return count")
	plotCountCmd.Flags().StringToStringVarP(&count_filters, "filter", "f", map[string]string{}, "Filter where COLUMN=VALUE")

	plotCountCmd.MarkFlagsMutuallyExclusive("group", "mode")

	plotCmd.AddCommand(plotCountCmd)
}
