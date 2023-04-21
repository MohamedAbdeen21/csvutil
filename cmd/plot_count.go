package csvutil

import (
	"io"
	"strings"

	"github.com/MohamedAbdeen21/csvutil/pkg/plotter"
	"github.com/spf13/cobra"
)

func plotCountCmd() *cobra.Command {
	data := make(map[string]int64)
	cmd := countCmd(&data)
	cmd.SetOut(io.Discard)
	column, _ := cmd.Flags().GetString("group")
	cmd.PostRunE = func(cmd *cobra.Command, args []string) error {
		var err error
		switch strings.ToLower(plotType) {
		case "bar":
			err = plotter.BarPlotGroup(column, data, outputDir)
		case "scatter":
			err = plotter.ScatterPlotGroup(column, data, outputDir)
		case "line":
			err = plotter.LinePlotGroup(column, data, outputDir)
		}
		if err != nil {
			return err
		}
		return nil
	}
	return cmd
}
