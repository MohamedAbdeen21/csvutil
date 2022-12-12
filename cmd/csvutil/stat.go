package csvutil

import (
	"os"
	"strings"

	"github.com/MohamedAbdeen21/csvutil/pkg/csvutil"
	"github.com/spf13/cobra"
)

var stats_string string
var statCmd = &cobra.Command{
	Use:     "stat",
	Short:   "Print stat about a column",
	Long:    "Print statistics like max, min, avg, and std_dev about a column",
	Args:    cobra.RangeArgs(1, 2),
	Example: "csvutil stat [flags] [file_name] [column_name]",
	Run: func(cmd *cobra.Command, args []string) {
		var result = make(map[string]float64)
		stats := strings.Split(stats_string, ",")

		if len(args) == 1 {
			result = csvutil.Stat(os.Stdin.Name(), args[0], 1, stats, delimiter)
		} else {
			result = csvutil.Stat(args[0], args[1], threads, stats, delimiter)
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
	statCmd.Flags().StringVarP(&stats_string, "stat", "s", strings.Join(statPossibleStats, ","), "The stat to display, default all")
	rootCmd.AddCommand(statCmd)
}
