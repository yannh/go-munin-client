package munin

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"strconv"
)

type Client struct {
	Fqdn string
	Port uint16
	conn       net.Conn
	br         *bufio.Reader
}

/*const (
	ERRLVL_INFO     = 1
	ERRLVL_WARNING  = 2
	ERRLVL_ERROR    = 3
	ERRLVL_CRITICAL = 4
)

type muninError struct {
  description  string
	level int16
}

func (e *muninError) Error() string {
	return e.description
}*/

func (n *Client) readMuninMultiline() ([]string, error) {
	var err error

	answer := make([]string, 0)

	for {
		line, err := n.br.ReadString('\n')
		if err != nil {
			if err != io.EOF || len(line) > 0 {
				return answer, err
			}
			break
		}
		if strings.EqualFold(line, ".\n") {
			break
		}

		answer = append(answer, line)
		//fmt.Printf(line)
	}

	return answer, err
}

func (n *Client) Connect() error {
	var err error

	n.conn, err = net.Dial("tcp", n.Fqdn+":"+strconv.FormatInt(int64(n.Port), 10))
	if err != nil {
		return err
	}

	// Reading & printing welcome string
	n.br = bufio.NewReader(n.conn)
	_, err = n.br.ReadString('\n')
	if err != nil {
		return fmt.Errorf("Error reading welcome message: %v", err)
	}

	return nil
}

func (n *Client) CloseConnection() error {
	return n.conn.Close()
}

func (n *Client) getPluginList() ([]string, error) {
	if n.conn == nil {
		return make([]string, 0), fmt.Errorf("Error getting plugin list from %s: no connection to server", n.Fqdn)
	}

	if _, err := fmt.Fprintf(n.conn, "list\n"); err != nil {
		return make([]string, 0), fmt.Errorf("Error getting plugin list from %s: %v", n.Fqdn, err)
	}

	line, err := n.br.ReadString('\n')
	if err != nil {
		return make([]string, 0), err
	}

	return strings.Fields(line), nil
}

func (n *Client) FetchPlugin(plugin string, outputChannel chan<- map[string]string, errorChannel chan<- error) error {

	pluginValuesTypes := make(map[string]string)

	if n.conn == nil {
		return fmt.Errorf("Error fetching plugin %s from %s: no connection to server", plugin, n.Fqdn)
	}

	if _, err := fmt.Fprintf(n.conn, "config "+plugin+"\n"); err != nil {
		return fmt.Errorf("Error fetching plugin %s from %s: %v", plugin, n.Fqdn, err)
	}
	pluginRawOutput, _ := n.readMuninMultiline()

	for _, line := range pluginRawOutput {
		if strings.Contains(line, ".type") == true {
			valueName := strings.Split(line, ".")[0]
			valueType := strings.Fields(line)[1]
			pluginValuesTypes[valueName] = valueType
		}
	}

	fmt.Fprintf(n.conn, "fetch "+plugin+"\n")
	pluginRawOutput, _ = n.readMuninMultiline()

	for _, line := range pluginRawOutput {
		pluginValue := make(map[string]string)

		lineFields := strings.Fields(line)
		if len(lineFields) < 1 {
			errorChannel <- fmt.Errorf("Unexpected line in Munin output: %s", line)
			runtime.Goexit()
		}
		vName := strings.Split(lineFields[0], ".")
		pluginValue["name"] = plugin + "." + vName[0]

		if _, typeDefined := pluginValuesTypes[vName[0]]; typeDefined == true {
			pluginValue["type"] = pluginValuesTypes[vName[0]]
		} else {
			pluginValue["type"] = "GAUGE"
		}

		pluginValue["value"] = lineFields[1]
		pluginValue["fqdn"] = n.Fqdn
		outputChannel <- pluginValue
	}

	return nil
}

func (n *Client) FetchAllPlugins(outputChannel chan<- map[string]string, errorChannel chan<- error) {

	pluginList, err := n.getPluginList()
	if err != nil {
		errorChannel <- err
		return
	}

	for _, plugin := range pluginList {
		if err := n.FetchPlugin(plugin, outputChannel, errorChannel); err != nil {
			errorChannel <- err
		}
	}
}
