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

		if !csvutil.ExistsIn(mode, CountPossibleModes) {
			panic("Mode must be one of the possible values")
		}

		if group != "" {
			mode = "group"
		}

		option := csvutil.Options{
			Mode:      mode,
			Filters:   count_filters,
			Group:     group,
			Delimiter: delimiter,
			Threads:   threads,
		}

		// use stdin
		if len(args) == 0 {
			fd := csvutil.CopyToTemp(cmd.InOrStdin())
			defer os.Remove(fd.Name())
			defer fd.Close()
			option.Filename = fd.Name()
		} else {
			option.Filename = args[0]
		}

		count, err := csvutil.Count(&option)

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
	plotCountCmd.Flags().StringVarP(&mode, "mode", "m", "lines", fmt.Sprintf("What to count\n%v", CountPossibleModes))
	plotCountCmd.Flags().StringVarP(&group, "group", "g", "", "Group by column and return count")
	plotCountCmd.Flags().StringToStringVarP(&count_filters, "filter", "f", map[string]string{}, "Filter where COLUMN=VALUE")

	plotCountCmd.MarkFlagsMutuallyExclusive("group", "mode")

	plotCmd.AddCommand(plotCountCmd)
}
