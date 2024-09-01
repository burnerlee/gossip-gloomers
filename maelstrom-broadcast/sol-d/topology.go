package main

var topology = map[string][]string{
	"n0":  {},
	"n1":  {"n0"},
	"n2":  {"n1", "n3"},
	"n3":  {"n4"},
	"n4":  {},
	"n5":  {"n0"},
	"n6":  {"n5", "n1"},
	"n7":  {"n6", "n2", "n8"},
	"n8":  {"n9", "n3"},
	"n9":  {"n4"},
	"n10": {"n5", "n15"},
	"n11": {"n6", "n10", "n16"},
	"n12": {"n7", "n11", "n13", "n17"},
	"n13": {"n8", "n14", "n18"},
	"n14": {"n9", "n19"},
	"n15": {"n20"},
	"n16": {"n15", "n21"},
	"n17": {"n16", "n18", "n22"},
	"n18": {"n19", "n23"},
	"n19": {"n24"},
	"n20": {},
	"n21": {"n20"},
	"n22": {"n21", "n23"},
	"n23": {"n24"},
	"n24": {},
}

func getTopologyNeighbours(node string) []string {
	return topology[node]
}
