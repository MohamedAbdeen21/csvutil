package csvutil

import (
	"os"
	"strings"

	csvutil "github.com/MohamedAbdeen21/csvutil/pkg"
	"github.com/spf13/cobra"
)

var stats_string string

var statCmd = &cobra.Command{
	Use:     "stat",
	Short:   "Print statistics about a column",
	Long:    "Print statistics like max, min, avg, and std_dev about a column",
	Args:    cobra.RangeArgs(1, 2),
	Example: "csvutil stat [flags] [file_name] [column_name]",
	Run: func(cmd *cobra.Command, args []string) {
		var result = make(map[string]float64)
		stats := strings.Split(stats_string, ",")

		option := csvutil.Options{
			Stats:     stats,
			Delimiter: delimiter,
		}

		if len(args) == 1 {
			cmd.Print(">")
			option.Filename = os.Stdin.Name()
			option.Columns = []string{args[0]}
			option.Threads = 1
		} else {
			option.Filename = args[0]
			option.Columns = []string{args[1]}
			option.Threads = threads
		}

		result, err := csvutil.Stat(&option)

		if err != nil {
			cmd.PrintErrln(err)
			os.Exit(1)
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
	RootCmd.AddCommand(statCmd)
}
