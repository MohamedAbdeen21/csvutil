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
	cmd.PostRun = func(cmd *cobra.Command, args []string) {
		switch strings.ToLower(plotType) {
		case "bar":
			plotter.BarPlotGroup(column, data, outputDir)
		case "scatter":
			plotter.ScatterPlotGroup(column, data, outputDir)
		case "line":
			plotter.LinePlotGroup(column, data, outputDir)
		}
	}
	return cmd
}
