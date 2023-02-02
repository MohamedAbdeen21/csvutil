package csvutil

import (
	"fmt"
	"os"
	"strings"

	csvutil "github.com/MohamedAbdeen21/csvutil/pkg"
	"github.com/spf13/cobra"
)

var mode string
var group string
var count_filters map[string]string
var count_nulls string

func countCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "count",
		Short: "Count the number of lines, bytes or frequency of column values",
		Args:  cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {

			if !csvutil.ExistsIn(mode, CountPossibleModes) {
				return fmt.Errorf("mode must be one of the possible values %v", CountPossibleModes)
			}

			if cmd.Flags().Changed("group") {
				mode = "group"
			}

			filter := make(map[string][]string)
			for key, value := range count_filters {
				values := strings.Split(value, "||")
				filter[key] = values
			}

			option := csvutil.Options{
				Mode:      mode,
				Filters:   filter,
				Group:     group,
				Delimiter: delimiter,
				Threads:   threads,
				Nulls:     count_nulls,
			}

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
				return err
			}

			for key, value := range count {
				cmd.OutOrStdout().Write([]byte(fmt.Sprintf("%s: %d\n", key, value)))
			}
			return nil
		},
	}
	cmd.Flags().
		StringVarP(&mode, "mode", "m", "lines", fmt.Sprintf("What to count\n%v", CountPossibleModes))
	cmd.Flags().StringVarP(&group, "group", "g", "", "Group by column and return count")
	cmd.Flags().
		StringToStringVarP(&count_filters, "filter", "f", map[string]string{}, "Filter where COLUMN=\"VALUE1||VALUE2||...\"")
	cmd.Flags().StringVarP(&count_nulls, "nulls", "n", "", "String to be considered as Null")
	cmd.MarkFlagsMutuallyExclusive("group", "mode")
	return cmd
}
