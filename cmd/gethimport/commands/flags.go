package commands

import (
	"github.com/spf13/cobra"
)

var (
	datadirCli        string
	chain             string
	databaseVerbosity int
	gethDatadir       string
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func withDataDir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&datadirCli, "datadir", "", "data directory for temporary ELT files")
	must(cmd.MarkFlagRequired("datadir"))
	must(cmd.MarkFlagDirname("datadir"))

	cmd.Flags().StringVar(&gethDatadir, "geth.datadir", "", "path to the geth db")
	must(cmd.MarkFlagRequired("geth.datadir"))
	must(cmd.MarkFlagDirname("geth.datadir"))

	cmd.Flags().IntVar(&databaseVerbosity, "database.verbosity", 2, "Enabling internal db logs. Very high verbosity levels may require recompile db. Default: 2, means warning")
}

func withChain(cmd *cobra.Command) {
	cmd.Flags().StringVar(&chain, "chain", "mainnet", "pick a chain to assume (mainnet, sepolia, etc.)")
	must(cmd.MarkFlagRequired("chain"))
}
