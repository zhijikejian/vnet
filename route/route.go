package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"util"
)

var vnetids_sm_ = util.NewSyncMap()   // map[int]struct{} // vnet连接映射的id列表
var vnetconns_sm_ = util.NewSyncMap() // map[string]net.Conn // vnet连接映射，{vnetid:net.Conn}
var vnetlist_sm_ = util.NewSyncMap()  // map[string]map[string]string // vnet列表，{vnetid:{name:name,group:group}}
var serveids_sm_ = util.NewSyncMap()  // map[int]struct{} // vnet申请的serve的id列表
var servelist_sm_ = util.NewSyncMap() // map[string]map[string]util.Serve // 保存的所有vnet的服务信息，{vnetid:{serveid:util.Serve}}
var servemap_sm_ = util.NewSyncMap()  // map[string]string // 服务路由，{serveid:vnetid}
var hearttime_sm_ = util.NewSyncMap() // map[string]time.Second // 记录各客户端心跳时间，{vnetid:int64}

var lastvnetmsgtime_sm_ = util.NewSyncMap() // map[string]time.Now().UnixNano() // vnetmsg的最新接收时间，用来做超时判断
func updatevnetmsgtime(vnetid string) {
	lastvnetmsgtime_sm_.Store(vnetid, time.Now().UnixNano())
}

type Connlink struct {
	vnetid1 string // vnetid or another vnetid
	vnetid2 string // another vnetid or vnetid
}

var connlinkmap_sm_ = util.NewSyncMap() // map[string]Connlink{} // 链接路由 {vnetconnid:{vnetid1:vnetid,vnetid2:tovnetid}} // 当有一方vnet断链时，向另一方tovnet发送结束消息

var rouchan = make(chan util.RouteMessage, 5) // 消息管道 // 改为仅发送配置类消息 // 配置管道

func init() {
	if len(os.Args) >= 2 {
		util.Configfile = os.Args[len(os.Args)-1]
	}
	fmt.Println("reading", util.Configfile, "...")
	if !util.Loadconfig() {
		fmt.Println(util.Configfile, "has err")
		os.Exit(0)
	}
}

func main() {
	fmt.Println(time.Now().Format("[2006-01-02 15:04:05]"), "route start ...")
	listen, err := net.Listen("tcp", util.Routeaddr.Ip+":"+util.Routeaddr.Port)
	if err != nil {
		fmt.Println("route listen err:", err)
		return
	}
	defer listen.Close()
	fmt.Println("route listening", util.Routeaddr.Ip+":"+util.Routeaddr.Port, "...\n")
	go route_config_handle() // 配置控制
	go timercheck()          // 心跳超时检测 定时检测
	go func() {              // 没用的东西，只是可以在终端输入空格
		var reader = bufio.NewReader(os.Stdin)
		for {
			reader.ReadString('\n')
		}
	}()
	// 建立连接
	for {
		vnetconn, err := listen.Accept()
		if err != nil {
			fmt.Println("route accept err:", err)
			break
		}
		if vnetconns_sm_.Len() > util.Vnetslimit {
			vnetconn.Close()
			continue
		}
		if id, ok := util.Generateid_sm_(vnetids_sm_.Keys(), util.Vnetslimit); ok {
			// 消息接收
			go vnet_readmsg_handle(id, vnetconn)
		} else {
			vnetconn.Close()
			continue
		}
	}
	fmt.Println("\nroute exit ---")
	os.Exit(0)
}

// vnet read -> rouchan
func vnet_readmsg_handle(id int, vnetconn net.Conn) {
	defer vnetconn.Close()
	vnetids_sm_.Store(id, struct{}{})
	vnetid := util.Fixinttostr(id, util.Msgvnetidsize)
	// 心跳初始计时
	hearttime_sm_.Store(vnetid, int64(time.Now().Unix()))
	vnetconns_sm_.Store(vnetid, vnetconn)
	servelist_sm_.Store(vnetid, map[string]util.Serve{}) // 更新servelist
	fmt.Println(time.Now().Format("[2006-01-02 15:04:05]"), "vnets+:", vnetconns_sm_.Len(), displayvnets())
	// 接收数据包
	var messagecache []byte
	var messagelen int = -1
	for {
		var buffer [util.Buffersize]byte
		bufsize, err := vnetconn.Read(buffer[:])
		if err != nil {
			break
		}
		// 更新vnetmsg的最新接收时间
		updatevnetmsgtime(vnetid)
		// 追加到缓存
		messagecache = append(messagecache, buffer[:bufsize]...)
		// 检查缓存和发送，当缓存长度少于msglen则等待下一条msg
		for {
			// 判断msglen，<0则更新
			if messagelen < 0 {
				if len(messagecache) >= util.Msglensize { // 缓存长度大于msglensize，则更新，否则break
					messagelenint, err := strconv.Atoi(string(messagecache[:util.Msglensize]))
					if err != nil {
						// 转int失败的话，则表示是错误msg，需做错误处理
						messagecache = []byte{} // 清空缓存
						messagelen = -1         // 重置msglen，貌似不需要
						break                   // 退出缓存循环
					} else {
						messagelen = messagelenint // 正确保存msglen
					}
				} else { // 缓存长度<msglensize，等待下一次msg
					break
				}
			}
			// 发送缓存，有msglen，且缓存长度大于msglen
			if len(messagecache) >= messagelen {
				// 发送缓存
				vnet_writemsg_handle(util.RouteMessage{Vnetid: vnetid, Msg: messagecache[:messagelen]}) // 发送到消息write，不去掉头部长度信息
				messagecache = append([]byte{}, messagecache[messagelen:]...)                           // 更新缓存
				messagelen = -1                                                                         // 重置msglen
			} else { // 缓存长度<msglen，等待下一次msg
				break
			}
		}
	}
	// 断链后的清理和维护 --------------
	// 删除vnet的id和连接映射
	vnetids_sm_.Delete(id)
	vnetconns_sm_.Delete(vnetid)
	fmt.Println(time.Now().Format("[2006-01-02 15:04:05]"), "vnets-:", vnetconns_sm_.Len(), displayvnets())
	// 当有一方vent掉线，则应立即向和他有conn工作关系的tovnet进行断链
	for _, kv := range connlinkmap_sm_.KVlist() {
		vnetconnid, connlink := kv[0].(string), kv[1].(Connlink)
		if connlink.vnetid1 == vnetid || connlink.vnetid2 == vnetid {
			// 发送关闭消息
			rouchan <- util.RouteMessage{
				Vnetid: vnetid,
				Msg:    util.Addmsglen(vnetconnid + util.Ctrlcloseconn + util.Msgserveidempty),
			}
		}
	}
	// 当有一方vent掉线，则应当更新servemap，servelist，并广播serves
	if serves, ok := servelist_sm_.Load(vnetid); ok {
		for _, serve := range serves.(map[string]util.Serve) {
			servemap_sm_.Delete(serve.Serveid)
		}
	}
	servelist_sm_.Delete(vnetid) // 更新servelist
	go allsendserves()           // 广播serves
	// 当有一方vent掉线，则应当更新 vnetlist
	vnetlist_sm_.Delete(vnetid)
	// 当有一方vent掉线，则应当结束心跳计时
	hearttime_sm_.Delete(vnetid)
}

// vnet write // sync ctrl close
func vnet_writemsg_handle(roumsg util.RouteMessage) {
	if message, ok := util.Parsemsg_vnet(roumsg.Msg); ok {
		switch message.Ctrl {
		case util.Ctrlcloseconn:
			{
				// 只需要通知另一方vnet就行 // 从一个vnet来，去另一个vnet // 如果connlink中不存在，则忽略
				if connlink, ok := connlinkmap_sm_.Load(message.Vnetconnid); ok {
					connlink := connlink.(Connlink)
					var tovnetid string
					if connlink.vnetid1 == roumsg.Vnetid {
						tovnetid = connlink.vnetid2
					} else {
						tovnetid = connlink.vnetid1
					}
					if vnetconn, ok := vnetconns_sm_.Load(tovnetid); ok {
						writemsg(util.Addmsglen(message.Vnetconnid+util.Ctrlcloseconn+util.Msgserveidempty), vnetconn.(net.Conn), message) // 将serveid写空，标志route端的关闭消息
					} else {
						fmt.Println("Ctrlcloseconn tovnetid err", tovnetid)
					}
					// 链路维护
					connlinkmap_sm_.Delete(message.Vnetconnid)
				} else {
					// fmt.Println("Ctrlcloseconn vnetconnid err", roumsg.Vnetid, message.Vnetconnid, connlinkmap_sm_.KVlist()) // 来源vnetid，链路vnetconnid，链路映射
				}
			}
		case util.Ctrlsendmsg:
			{
				// 根据serveid寻找tovnetid，根据vnetid和tovnetid来判断消息是请求还是回传
				if tovnetid, ok := servemap_sm_.Load(message.Serveid); ok {
					tovnetid := tovnetid.(string)
					if roumsg.Vnetid == tovnetid { // 回传消息
						if vnetconn, ok := vnetconns_sm_.Load(message.Vnetid); ok {
							writemsg(message.Msg, vnetconn.(net.Conn), message)
						} else {
							// fmt.Println("Ctrlsendmsg tovnetid err", message.Vnetid)
						}
					} else { // 请求消息
						if tovnetconn, ok := vnetconns_sm_.Load(tovnetid); ok {
							writemsg(message.Msg, tovnetconn.(net.Conn), message)
						} else {
							// fmt.Println("Ctrlsendmsg tovnetid err", tovnetid)
						}
					}
					// 链路维护 // 仅当请求消息时维护
					if roumsg.Vnetid != tovnetid {
						if _, ok := connlinkmap_sm_.Load(message.Vnetconnid); !ok {
							connlinkmap_sm_.Store(message.Vnetconnid, Connlink{vnetid1: message.Vnetid, vnetid2: tovnetid})
						}
					}
				} else {
					// fmt.Println("Ctrlsendmsg serveid err", message.Serveid, servemap_sm_.KVlist())
				}
			}
		case util.Ctrlsetvnetid, util.Ctrlgetserveid, util.Ctrlregserveid, util.Ctrldelserveid, util.Ctrlgetserves, util.Ctrlheartbeat, util.Ctrlsendmsg_noserve, util.Ctrlvnetinfo, util.Ctrlgetvnetsinfo:
			{
				// 控制类的消息，转发至rouchan配置中心
				go func(roumsg util.RouteMessage) {
					rouchan <- roumsg
				}(roumsg)
			}
		default:
			{
				fmt.Println("switch message.Ctrl err default")
			}
		}
	} else {
		fmt.Println("Parsemsg_vnet message err", string(roumsg.Msg))
	}
}

// config ctrl
func route_config_handle() {
	for roumsg := range rouchan {
		if message, ok := util.Parsemsg_vnet(roumsg.Msg); ok {
			switch message.Ctrl {
			case util.Ctrlsetvnetid:
				{
					// vnet获取自己的vnetid
					if vnetconn, ok := vnetconns_sm_.Load(roumsg.Vnetid); ok {
						writemsg(util.Addmsglen(roumsg.Vnetid+util.Msgconnidempty+util.Ctrlsetvnetid+util.Msgserveidempty), vnetconn.(net.Conn), message)
					} else {
						fmt.Println("Ctrlsetvnetid vnetid err", roumsg.Vnetid)
					}
				}
			case util.Ctrlgetserveid:
				{
					// 生成一个和已注册的服务的serveid不重复的id
					if vnetconn, ok := vnetconns_sm_.Load(roumsg.Vnetid); ok {
						id := 0
						if id_, ok := util.Generateid_sm_(serveids_sm_.Keys(), util.Serveslimit); ok {
							id = id_
						} else {
							fmt.Println("Ctrlgetserveid serveid err already full", roumsg.Vnetid)
						}
						serveid := util.Fixinttostr(id, util.Msgserveidsize)
						writemsg(util.Addmsglen(roumsg.Vnetid+util.Msgconnidempty+util.Ctrlgetserveid+serveid), vnetconn.(net.Conn), message)
						if serveid != util.Msgserveidempty {
							serveids_sm_.Store(id, struct{}{}) // 加入serveids
							go func() {
								// 10s 后还没有注册这个id，则删除
								time.Sleep(time.Second * 10)
								if _, ok := servemap_sm_.Load(serveid); !ok {
									serveids_sm_.Delete(id)
								}
							}()
						}
					} else {
						fmt.Println("Ctrlgetserveid vnetid err", roumsg.Vnetid)
					}
				}
			case util.Ctrlregserveid:
				{
					// vnet上传自己的serve，route进行保存servelist，并生成servemap映射
					var allsendservesflag = false // 是否需要广播服务
					var serve = util.Serve{}
					err := json.Unmarshal(roumsg.Msg[util.Msglen_toserveidsize:], &serve)
					if err != nil {
						fmt.Println("Ctrlregserveid json.Unmarshal Serve err", err)
						break
					}
					// 检查serveid是否已存在，主要在servemap中查
					var serveidres = util.Msgserveidempty + serve.Serveid
					if _, ok := servemap_sm_.Load(serve.Serveid); !ok { // 不在servemap
						if _, ok := servelist_sm_.Load(roumsg.Vnetid); !ok { // servelist没有vnetid
							servelist_sm_.Store(roumsg.Vnetid, map[string]util.Serve{})
						}
						if vnetservelist, ok := servelist_sm_.Load(roumsg.Vnetid); ok {
							vnetservelist := vnetservelist.(map[string]util.Serve)
							vnetservelist[serve.Serveid] = serve // 保存入servelist
							servelist_sm_.Store(roumsg.Vnetid, vnetservelist)
						}
						servemap_sm_.Store(serve.Serveid, roumsg.Vnetid) // 添加servemap
						serveidres = serve.Serveid                       // 如果成功，则serveid != "00"
						allsendservesflag = true                         // 需要广播服务
					}
					// 回传消息
					if vnetconn, ok := vnetconns_sm_.Load(roumsg.Vnetid); ok {
						writemsg(util.Addmsglen(roumsg.Vnetid+util.Msgconnidempty+util.Ctrlregserveid+serveidres), vnetconn.(net.Conn), message)
					} else {
						fmt.Println("Ctrlsetvnetid vnetid err", roumsg.Vnetid)
					}
					// 广播服务变更
					if allsendservesflag {
						go allsendserves()
					}
				}
			case util.Ctrldelserveid:
				{
					// vnet删除自己的serve，route进行删除servelist，并删除servemap映射
					var allsendservesflag = false // 是否需要广播服务
					var msgback = roumsg.Vnetid + util.Msgconnidempty + util.Ctrldelserveid + util.Msgserveidempty + message.Serveid
					if _, ok := servelist_sm_.Load(roumsg.Vnetid); ok {
						if vnetservelist, ok := servelist_sm_.Load(roumsg.Vnetid); ok {
							vnetservelist := vnetservelist.(map[string]util.Serve)
							delete(vnetservelist, message.Serveid)
							servelist_sm_.Store(roumsg.Vnetid, vnetservelist)
						}
						servemap_sm_.Delete(message.Serveid)
						msgback = roumsg.Vnetid + util.Msgconnidempty + util.Ctrldelserveid + message.Serveid
						allsendservesflag = true // 需要广播服务
					}
					// 中断所有的serveid提供的链路 // vnet端进行
					// 回传消息
					if vnetconn, ok := vnetconns_sm_.Load(roumsg.Vnetid); ok {
						writemsg(util.Addmsglen(msgback), vnetconn.(net.Conn), message)
					} else {
						fmt.Println("Ctrldelserveid vnetid err", roumsg.Vnetid)
					}
					// 广播服务变更
					if allsendservesflag {
						go allsendserves()
					}
				}
			case util.Ctrlgetserves:
				{
					// 将servelist整理成列表 // 或传输jsonstr或inistr到vnet端再解析为列表展示
					if vnetconn, ok := vnetconns_sm_.Load(roumsg.Vnetid); ok {
						var servelist_ = map[string]map[string]util.Serve{}
						if vnetinfo_, ok := vnetlist_sm_.Load(roumsg.Vnetid); ok {
							if vnetgroup, ok := vnetinfo_.(map[string]string)["group"]; ok {
								for _, kv := range vnetlist_sm_.KVlist() {
									vnetid, vnetinfo := kv[0].(string), kv[1].(map[string]string)
									if vnetinfo["group"] == vnetgroup {
										if vnetservelist, ok := servelist_sm_.Load(vnetid); ok {
											servelist_[vnetid] = vnetservelist.(map[string]util.Serve)
										}
									}
								}
							}
						}
						servesjson, err := json.Marshal(servelist_)
						if err != nil {
							fmt.Println("json.Marshal servelist err", err)
							servesjson = []byte(roumsg.Vnetid + util.Msgconnidempty + util.Ctrlgetserves + util.Msgserveidempty)
						} else {
							servesjson = append([]byte(roumsg.Vnetid+util.Msgconnidempty+util.Ctrlgetserves+util.Msgserveidempty), servesjson...)
						}
						writemsg(util.Addmsglen(servesjson), vnetconn.(net.Conn), message)
					} else {
						fmt.Println("Ctrlgetserves vnetid err", roumsg.Vnetid)
					}
				}
			case util.Ctrlheartbeat:
				{
					// 原样回传
					if vnetconn, ok := vnetconns_sm_.Load(roumsg.Vnetid); ok {
						writemsg(roumsg.Msg, vnetconn.(net.Conn), message)
					} else {
						fmt.Println("Ctrlheartbeat vnetid err", roumsg.Vnetid)
					}
					// 心跳更新计时
					hearttime_sm_.Store(roumsg.Vnetid, int64(time.Now().Unix()))
				}
			case util.Ctrlsendmsg_noserve:
				{
					// 无注册服务通信，将serveid位当做tovnetid // 不是同组不转发 或者 转发但让对方拒绝？
					var unserve_tovnetid = message.Serveid[1:] // vnetid.len=2, serveid.len=3
					if message.Serveid[2] == '{' {             // 临时
						msg := string(message.Msg[4:11])
						msg = msg + "0"
						message.Msg = util.Addmsglen(append([]byte(msg), message.Msg[11:]...))
						unserve_tovnetid = string(message.Msg[12:14]) // vnetid.len=2, serveid.len=3
					}
					if vnetinfo, ok := vnetlist_sm_.Load(roumsg.Vnetid); ok {
						if tovnetinfo, ok := vnetlist_sm_.Load(unserve_tovnetid); ok {
							if vnetgroup, ok := vnetinfo.(map[string]string)["group"]; ok {
								if tovnetgroup, ok := tovnetinfo.(map[string]string)["group"]; ok {
									if vnetgroup != tovnetgroup {
										break // 直接不转发
									}
								}
							}
						}
					}
					if tovnetconn, ok := vnetconns_sm_.Load(unserve_tovnetid); ok {
						writemsg(message.Msg, tovnetconn.(net.Conn), message)
					} else {
						fmt.Println("Ctrlsendmsg_noserve tovnetid(serveid) err")
					}
				}
			case util.Ctrlvnetinfo:
				{
					// 将上传的vnet信息更新到vnetlist
					var msgback = roumsg.Vnetid + util.Msgconnidempty + util.Ctrlvnetinfo + util.Msgserveidempty + util.Msgserveidempty
					var vnetinfomap = make(map[string]string)
					err := json.Unmarshal(message.Msg[util.Msglen_toserveidsize:], &vnetinfomap)
					if err != nil {
						fmt.Println("json.Unmarshal vnetinfomap err", err)
					} else {
						vnetlist_sm_.Store(roumsg.Vnetid, vnetinfomap)
						msgback = msgback[:len(msgback)-len(util.Msgserveidempty)]
					}
					// 回传消息
					if vnetconn, ok := vnetconns_sm_.Load(roumsg.Vnetid); ok {
						writemsg(util.Addmsglen(msgback), vnetconn.(net.Conn), message)
					} else {
						fmt.Println("Ctrlvnetinfo vnetid err", roumsg.Vnetid)
					}
				}
				// 广播服务变更 // 以免在前面服务广播时，vnetlist没有更新，这里
				go allsendserves()
			case util.Ctrlgetvnetsinfo:
				{
					// 将vnetlist整理成列表 // 或传输jsonstr或inistr到vnet端再解析为列表展示
					if vnetconn, ok := vnetconns_sm_.Load(roumsg.Vnetid); ok {
						var vnetlist_ = make(map[string]string)
						if vnetinfo_, ok := vnetlist_sm_.Load(roumsg.Vnetid); ok {
							if vnetgroup, ok := vnetinfo_.(map[string]string)["group"]; ok {
								for _, kv := range vnetlist_sm_.KVlist() {
									vnetid, vnetinfo := kv[0].(string), kv[1].(map[string]string)
									if vnetinfo["group"] == vnetgroup {
										vnetlist_[vnetid] = "name:" + vnetinfo["name"] + ", group:" + vnetinfo["group"] // vnetinfo["name"] + vnetinfo["group"] + vnetinfo["other"]
									}
								}
							}
						}
						vnetsjson, err := json.Marshal(vnetlist_)
						if err != nil {
							fmt.Println("json.Marshal vnetlist err", err)
							vnetsjson = []byte(roumsg.Vnetid + util.Msgconnidempty + util.Ctrlgetvnetsinfo + util.Msgserveidempty)
						} else {
							vnetsjson = append([]byte(roumsg.Vnetid+util.Msgconnidempty+util.Ctrlgetvnetsinfo+util.Msgserveidempty), vnetsjson...)
						}
						writemsg(util.Addmsglen(vnetsjson), vnetconn.(net.Conn), message)
					} else {
						fmt.Println("Ctrlgetvnetsinfo vnetid err", roumsg.Vnetid)
					}
				}
			default:
				{
					fmt.Println("switch message.Ctrl err default")
				}
			}
		} else {
			fmt.Println("Parsemsg_vnet message err", string(roumsg.Msg))
		}
	}
}

// 复用conn.write代码
func writemsg(msg []byte, conn net.Conn, message util.VnetMessage) bool {
	_, err := conn.Write(msg)
	if err != nil {
		return false
	}
	return true
}

// 广播服务变更
func allsendserves() {
	for _, vnetid := range vnetconns_sm_.Keys() {
		vnetid := vnetid.(string)
		rouchan <- util.RouteMessage{
			Vnetid: vnetid,
			Msg:    util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrlgetserves + util.Msgserveidempty),
		}
	}
	// 附加一个同步serveids
	syncserveids()
}

// 同步serveids
func syncserveids() {
	for _, id := range serveids_sm_.Keys() {
		id := id.(int)
		serveid := util.Fixinttostr(id, util.Msgserveidsize)
		if _, ok := servemap_sm_.Load(serveid); !ok {
			serveids_sm_.Delete(id)
		}
	}
}

// 格式化显示vnets信息，[[vnetid,vnetaddr],[vnetid,vnetaddr]]
func displayvnets() [][]any {
	var displayvnets = [][]any{}
	for _, kv := range vnetconns_sm_.KVlist() {
		vnetid, vnetconn := kv[0].(string), kv[1].(net.Conn)
		displayvnets = append(displayvnets, []any{vnetid, vnetconn.RemoteAddr().String()})
	}
	return displayvnets
}

// 心跳超时检测 // 检测最新活跃
func timercheck() {
	for {
		time.Sleep(time.Second)
		for _, kv := range lastvnetmsgtime_sm_.KVlist() {
			vnetid, lasttime := kv[0].(string), kv[1].(int64)
			if time.Now().Unix()-lasttime > 600 { // 超过 12s 关闭与vnet端的连接
				if vnetconn, ok := vnetconns_sm_.Load(vnetid); ok {
					fmt.Println("vnet", vnetid, "连接超时 ...")
					vnetconn.(net.Conn).Close()
				}
			}
		}
	}
}
