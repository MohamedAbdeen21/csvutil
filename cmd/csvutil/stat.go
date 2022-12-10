package csvutil

import (
	"fmt"

	"github.com/MohamedAbdeen21/csvutil/pkg/csvutil"
	"github.com/spf13/cobra"
)

var stats []string
var statCmd = &cobra.Command{
	Use:   "stat",
	Short: "Print stat about a column",
	Long:  "Print statistics like max, min, avg, and std_dev about a column",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		result := csvutil.Stat(args[0], threads, delimiter, args[1], stats)
		for key, value := range result {
			if csvutil.ExistsIn(key, stats) {
				fmt.Printf("%-8s: %.2f\n", key, value)
			}
		}
	},
}

func init() {
	statCmd.Flags().StringArrayVarP(&stats, "stat", "s", []string{"max", "min", "mean", "avg", "sum", "std_dev"}, "The stat to display, default all")
	rootCmd.AddCommand(statCmd)
}
