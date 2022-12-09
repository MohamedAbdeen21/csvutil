package csvutil

import (
	"github.com/MohamedAbdeen21/csvutil/pkg/csvutil"
	"github.com/spf13/cobra"
)

var statCmd = &cobra.Command{
	Use:   "stat",
	Short: "Print stat about a column",
	Long:  "Print statistics like max, min, and avg about a column",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		csvutil.Stat(args[0], threads, delimiter, args[1])
	},
}

func init() {
	rootCmd.AddCommand(statCmd)
}
