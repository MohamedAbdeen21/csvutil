package csvutil

import (
	"os"
	"strings"

	csvutil "github.com/MohamedAbdeen21/csvutil/pkg"
	"github.com/spf13/cobra"
)

var stats_string string
var column string

var statCmd = &cobra.Command{
	Use:     "stat",
	Short:   "Print statistics about a numerical column",
	Long:    "Print statistics like max, min, avg, and std_dev about a numerical column",
	Args:    cobra.RangeArgs(0, 1),
	Example: "csvutil stat [flags] [file_name] -c [column_name]",
	RunE: func(cmd *cobra.Command, args []string) error {
		var result = make(map[string]float64)
		stats := strings.Split(stats_string, ",")

		option := csvutil.Options{
			Stats:     stats,
			Delimiter: delimiter,
		}

		if len(args) == 0 {
			cmd.Print(">")
			option.Filename = os.Stdin.Name()
			option.Columns = []string{column}
			option.Threads = 1
		} else {
			option.Filename = args[0]
			option.Columns = []string{column}
			option.Threads = threads
		}

		result, err := csvutil.Stat(&option)

		if err != nil {
			return err
		}

		for key, value := range result {
			if csvutil.ExistsIn(key, stats) {
				if csvutil.ExistsIn(key, intStats) {
					cmd.Printf("%-8s: %0.f\n", key, value)
				} else {
					cmd.Printf("%-8s: %.2f\n", key, value)
				}
			}
		}
		return nil
	},
}

func init() {
	statCmd.Flags().StringVarP(&stats_string, "stat", "s", strings.Join(statPossibleStats, ","), "The stat to display, default all")
	statCmd.Flags().StringVarP(&column, "column", "c", "", "The column to calculate stats on")
	statCmd.MarkFlagRequired("column")
}
