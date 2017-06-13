package dvara

import(
  corelog "github.com/intercom/gocore/log"
	"fmt"
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

type BaseProxyExtension struct {}

func (extension *BaseProxyExtension) onConnection(*ProxiedMessage) bool {
  return true;
}

func (extension *BaseProxyExtension) onHeader(*ProxiedMessage) bool {
	return true;
}

func (extension *BaseProxyExtension) onMessage(*ProxiedMessage) bool {
	return true;
}

func (extension *BaseProxyExtension) onResponse(*ProxiedMessage) bool {
	return true;
}

type QueryLogger struct {
	*BaseProxyExtension
}

func (extension *QueryLogger) onHeader(message *ProxiedMessage) bool {
	query := message.GetQuery()
	logMessage := fmt.Sprintf("message: {%s}", query)
	corelog.LogInfo(logMessage);
	return true;
}