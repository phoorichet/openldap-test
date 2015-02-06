package main

import (
	"fmt"
	"github.com/go-ldap/ldap"
	"math"
	"math/rand"
	"time"
)

var (
	ldapServer string   = "10.239.223.231"
	ldapPort   uint16   = 389
	baseDN     string   = "dc=C-NTDB"
	filter     string   = "(objectClass=*)"
	attributes []string = []string{"dn", "cn"}
	user       string   = "dc=C-NTDB"
	passwd     string   = "secret"
)

type LdapConnection struct {
	LdapServer string
	LdapPort   uint16
	BaseDN     string
	Attributes []string
	User       string
	Password   string
	Conn       *ldap.Conn
}

type LdapModifyRequest struct {
	Id      int
	Uid     int
	Request *ldap.ModifyRequest
}

type LdapSearchRequest struct {
	Id      int
	Uid     int
	Request *ldap.SearchRequest
}

func generateModifyRequest(minUid, maxUid, bufferSize, randNum int) chan *LdapModifyRequest {
	fmt.Printf("Generating Modify Requests...\n")
	out := make(chan *LdapModifyRequest, bufferSize)
	n := minUid
	allocRange := maxUid - minUid
	count := 0
	total := math.MaxInt32
	go func() {
		for {
			dn := fmt.Sprintf("amfProductNo=4001,subdata=profile,ds=amf,subdata=services,uid=%015d,ds=SUBSCRIBER,o=AIS,dc=C-NTDB", n)
			mr := ldap.NewModifyRequest(dn)
			mr.Replace("priority", []string{string(randNum)})
			out <- &LdapModifyRequest{Uid: n, Request: mr}
			n = (n + 1) % allocRange
			count++
			if count >= total {
				break
			}
		}
		close(out)
		fmt.Printf("Done generating...\n")
	}()
	return out
}

func fireModifyRequest(in chan *LdapModifyRequest, outBuffSize int, control chan int, conns []LdapConnection, id int) { //chan *ldap.SearchResult {
	fmt.Printf("Fire Search Request...\n")
	out1 := make(chan *LdapSearchRequest, outBuffSize)
	out2 := make(chan *LdapSearchRequest, outBuffSize)
	out3 := make(chan *LdapSearchRequest, outBuffSize)
	tick_chan := time.NewTicker(time.Second * 1).C
	time_chan := time.NewTimer(time.Minute * 60).C
	count := 0
	go func() {
		for {
			select {
			case m, more := <-in:
				if more {
					//fmt.Printf("%v -> %v\n", s, more)
					fireMod(m.Request, conns[0].Conn) // Fixed to Master

					// make two requests slaves
					sr := ldap.NewSearchRequest(
						fmt.Sprintf("amfProductNo=4001,subdata=profile,ds=amf,subdata=services,uid=%015d,ds=SUBSCRIBER,o=AIS,dc=C-NTDB", m.Uid),
						ldap.ScopeBaseObject,
						ldap.NeverDerefAliases, 0, 0, false,
						filter,
						attributes,
						nil)

					out1 <- &LdapSearchRequest{
						Id:      1, // Fixed to Slave 2
						Request: sr,
					}
					out2 <- &LdapSearchRequest{
						Id:      2, // Fixed to Slave 2
						Request: sr,
					}
					out3 <- &LdapSearchRequest{
						Id:      3, // Fixed to Slave 3
						Request: sr,
					}
					count++
				} else {
					control <- 1
					return
				}
			case sr, more := <-out1:
				if more {
					//fmt.Printf("%v -> %v\n", s, more)
					fireSearch(sr.Request, conns[sr.Id].Conn)
				} else {
					//control <- 1
					// return
				}
			case sr, more := <-out2:
				if more {
					//fmt.Printf("%v -> %v\n", s, more)
					fireSearch(sr.Request, conns[sr.Id].Conn)
				} else {
					// control <- 1
					// return
				}
			case sr, more := <-out3:
				if more {
					//fmt.Printf("%v -> %v\n", s, more)
					fireSearch(sr.Request, conns[sr.Id].Conn)
				} else {
					// control <- 1
					// return
				}
			case <-tick_chan:
				fmt.Printf("Worker id:=%d, Total=%d, outstanding=%d\n", id, count, len(in))
			case <-time_chan:
				//close(out)
				fmt.Printf("-------- Time Out! ---------\n")
				control <- 1
				return
			default:
				//fmt.Printf("Wating..\n")
			}

		}
	}()
	//return out
}

func fireMod(request *ldap.ModifyRequest, conn *ldap.Conn) int {
	err := conn.Modify(request)
	if err != nil {
		fmt.Printf("TestModify Error: %v\n", err)
		return 0
	}
	return 1
}

func generateSearchRequest(num int, cap int) chan *ldap.SearchRequest {
	fmt.Printf("Gen Serach Request...\n")
	out := make(chan *ldap.SearchRequest, cap)
	n := 0
	total := 1000000
	go func() {

		for {
			sr := ldap.NewSearchRequest(
				fmt.Sprintf("amfProductNo=4001,subdata=profile,ds=amf,subdata=services,uid=%015d,ds=SUBSCRIBER,o=AIS,dc=C-NTDB", n),
				ldap.ScopeBaseObject,
				ldap.NeverDerefAliases, 0, 0, false,
				filter,
				attributes,
				nil)
			out <- sr
			n = (n + 1) % num
			if n == total {
				break
			}
			//fmt.Printf("Added entry %16d, queue size=%d\n", n, len(out))
		}
		close(out)
		fmt.Printf("Done generating.\n")
	}()
	return out
}

func fireSearchRequest(in chan *ldap.SearchRequest, num int, control chan int, conn *ldap.Conn, id int) { //chan *ldap.SearchResult {
	fmt.Printf("Fire Search Request...\n")
	//out := make(chan *ldap.SearchResult, num)
	tick_chan := time.NewTicker(time.Second * 1).C
	time_chan := time.NewTimer(time.Minute * 60).C
	count := 0
	go func() {
		for {
			select {
			case s, more := <-in:
				if more {
					//fmt.Printf("%v -> %v\n", s, more)
					fireSearch(s, conn)
					count++
				} else {
					control <- 1
					return
				}
			case <-tick_chan:
				fmt.Printf("id:=%d, Total=%d, outstanding=%d\n", id, count, len(in))
			case <-time_chan:
				//close(out)
				fmt.Printf("-------- Time Out! ---------\n")
				control <- 1
				return
			//case <-out:
			//      //fmt.Printf("Got: %v\n", r)
			default:
				//fmt.Printf("Wating..\n")
			}

		}
	}()
	//return out
}

func fireSearch(request *ldap.SearchRequest, conn *ldap.Conn) *ldap.SearchResult {
	sr, err := conn.Search(request)
	if err != nil {
		fmt.Printf("TestSearchError: %v\n", err)
		//return nil
	}
	return sr
}

// Establish and bind all the connection in the array.
func MakeConnection(conns []*LdapConnection) {
	for _, conn := range conns {
		c, err := ldap.Dial("tcp", fmt.Sprintf("%s:%d", conn.LdapServer, conn.LdapPort))
		if err != nil {
			fmt.Printf("ERROR: %s\n", err.Error())
		}
		conn.Conn = c

		err = conn.Conn.Bind(conn.User, conn.Password)
		if err != nil {
			fmt.Printf("ERROR: Cannot bind: %s\n", err.Error())
			return
		}
	}
}

func main() {
	fmt.Printf("===== OpenLDAP Test =====\n")
	conns := []*LdapConnection{
		&LdapConnection{
			LdapServer: "10.239.223.231",
			LdapPort:   ldapPort,
			BaseDN:     baseDN,
			Attributes: attributes,
			User:       user,
			Password:   passwd,
		},
		&LdapConnection{
			LdapServer: "10.239.223.232",
			LdapPort:   ldapPort,
			BaseDN:     baseDN,
			Attributes: attributes,
			User:       user,
			Password:   passwd,
		},
		&LdapConnection{
			LdapServer: "10.239.223.233",
			LdapPort:   ldapPort,
			BaseDN:     baseDN,
			Attributes: attributes,
			User:       user,
			Password:   passwd,
		}}

	MakeConnection(conns)
	for _, conn := range conns {
		defer conn.Conn.Close()
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	priority := r.Int()
	// Generate different ranges of requests
	// Uid ranges form 1 to 666,666

	generateModifyRequest(1, 666666, 30000, priority)
	generateModifyRequest(666667, 1333333, 30000, priority)
	generateModifyRequest(1333334, 1999999, 30000, priority)

	total_requests := 1000000
	chan_cap := 30000
	num_concurrency := 16

	input := generateSearchRequest(total_requests, chan_cap)
	control := make(chan int)
	for i := 0; i < num_concurrency; i++ {
		fireSearchRequest(input, total_requests, control, l, i)
	}
	<-control

}
