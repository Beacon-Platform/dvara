package main

import (
	"dvara"
)

var ExtensionStackInstance dvara.ProxyExtensionStack = dvara.NewProxyExtensionStack(
	[]dvara.ProxyExtension{
		&dvara.QueryLogger{},
	},
)