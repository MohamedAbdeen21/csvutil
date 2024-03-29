package csvutil

import (
	"fmt"
	"os"
	"strings"

	"github.com/MohamedAbdeen21/csvutil/pkg"
	"github.com/MohamedAbdeen21/csvutil/pkg/utility"
	"github.com/spf13/cobra"
)

var mode string
var group string
var count_filters map[string]string
var count_nulls string

func countCmd(return_copy *map[string]int) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "count",
		Short:   "Count the number of lines or bytes, or frequency of values in a column",
		Args:    cobra.RangeArgs(0, 1),
		Example: "csvutil count file.csv -g age -n 0\ncsvutil count file.csv -m lines -f age=30",
		RunE: func(cmd *cobra.Command, args []string) error {

			if !utility.ExistsIn(mode, countPossibleModes) {
				return fmt.Errorf("mode must be one of the possible values %v", countPossibleModes)
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
				fd := utility.CopyToTemp(cmd.InOrStdin())
				defer os.Remove(fd.Name())
				defer fd.Close()
				option.Filename = fd.Name()
			} else {
				option.Filename = args[0]
			}

			count, err := csvutil.Count(&option)

			*return_copy = count

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
		StringVarP(&mode, "mode", "m", "lines", fmt.Sprintf("What to count\n%v", countPossibleModes))
	cmd.Flags().StringVarP(&group, "group", "g", "", "Count the frequency of values in a column")
	cmd.Flags().
		StringToStringVarP(&count_filters, "filter", "f", map[string]string{}, "Filter where COLUMN=\"VALUE1||VALUE2||...\"")
	cmd.Flags().StringVarP(&count_nulls, "nulls", "n", "", "String to be considered as Null")
	cmd.MarkFlagsMutuallyExclusive("group", "mode")
	return cmd
}
