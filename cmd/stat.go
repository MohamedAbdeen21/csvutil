package csvutil

import (
	"os"
	"strings"

	csvutil "github.com/MohamedAbdeen21/csvutil/pkg"
	"github.com/spf13/cobra"
)

var stats_string string
var column string

func statCmd() *cobra.Command {
	cmd := &cobra.Command{
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
				Columns:   []string{column},
				Threads:   threads,
			}

			if len(args) == 0 {
				fd := csvutil.CopyToTemp(cmd.InOrStdin())
				defer os.Remove(fd.Name())
				defer fd.Close()
				option.Filename = fd.Name()
			} else {
				option.Filename = args[0]
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
	cmd.Flags().StringVarP(&stats_string, "stat", "s", strings.Join(StatPossibleStats, ","), "The stat to display, default all")
	cmd.Flags().StringVarP(&column, "column", "c", "", "The column to calculate stats on")
	cmd.MarkFlagRequired("column")
	return cmd
}
