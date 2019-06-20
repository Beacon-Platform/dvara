package dvara

import (
	"fmt"
	corelog "github.com/intercom/gocore/log"
)

type ProxyExtension interface {
	onConnection(*ProxiedMessage) bool
	onHeader(*ProxiedMessage) bool
	onMessage(*ProxiedMessage) bool
	onResponse(*ProxiedMessage) bool
}

type ProxyExtensionStack struct {
	extensions []ProxyExtension
}

func NewProxyExtensionStack(extensions []ProxyExtension) ProxyExtensionStack {
	return ProxyExtensionStack{
		extensions: extensions,
	}
}

func (manager *ProxyExtensionStack) GetExtensions() []ProxyExtension {
	return manager.extensions
}

type BaseProxyExtension struct{}

func (extension *BaseProxyExtension) onConnection(*ProxiedMessage) bool {
	return true
}

func (extension *BaseProxyExtension) onHeader(*ProxiedMessage) bool {
	return true
}

func (extension *BaseProxyExtension) onMessage(*ProxiedMessage) bool {
	return true
}

func (extension *BaseProxyExtension) onResponse(*ProxiedMessage) bool {
	return true
}

type QueryLogger struct {
	*BaseProxyExtension
}

func (extension *QueryLogger) onHeader(m *ProxiedMessage) bool {
	query, err := m.GetQuery()
	if err == nil {
		var logMessage string

		collName := m.fullCollectionName
		if collName != nil && len(collName) > 0 && query != nil {
			collName = collName[:len(collName)-1]
			logMessage = fmt.Sprintf("message: op: %v coll: %s {%v}", m.header.OpCode, collName, *query)
		} else {
			logMessage = fmt.Sprintf("message: op: %v", m.header.OpCode)
		}
		corelog.LogInfo(logMessage)
	}
	return true
}
