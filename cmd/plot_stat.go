package csvutil

import (
	"os"
	"strings"

	csvutil "github.com/MohamedAbdeen21/csvutil/pkg"
	"github.com/spf13/cobra"
)

var plotStatCmd = &cobra.Command{
	Use:     "stat",
	Short:   "Print stat about a column",
	Long:    "Print statistics like max, min, avg, and std_dev about a column",
	Args:    cobra.RangeArgs(1, 2),
	Example: "csvutil stat [flags] [file_name] [column_name]",
	Run: func(cmd *cobra.Command, args []string) {
		var result = make(map[string]float64)
		stats := strings.Split(stats_string, ",")

		if len(args) == 1 {
			cmd.Print(">")
			result, _ = csvutil.Stat(&csvutil.Options{
				Filename:  os.Stdin.Name(),
				Columns:   []string{args[0]},
				Threads:   1,
				Stats:     stats,
				Delimiter: delimiter,
			})
		} else {
			result, _ = csvutil.Stat(&csvutil.Options{
				Filename:  args[0],
				Columns:   []string{args[1]},
				Threads:   threads,
				Stats:     stats,
				Delimiter: delimiter,
			})
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
	},
}

func init() {
	plotStatCmd.Flags().StringVarP(&stats_string, "stat", "s", strings.Join(statPossibleStats, ","), "The stat to display, default all")
	plotCmd.AddCommand(plotStatCmd)
}
