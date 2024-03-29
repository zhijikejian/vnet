package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"util"
)

// 链路维护

var remoteconnids_sm_ = util.NewSyncMap()            // map[string]struct{} // 已连接 remote connid列表 {vnetconnid:{}}
var remoteconnmap_sm_ = util.NewSyncMap()            // map[string]net.Conn // 已连接 remote connid映射 {vnetconnid:net.conn}
var remoteconnidtoserveidmap_sm_ = util.NewSyncMap() // map[string]string // connid(仅只remote端vnetconnid)与serveid的对应关系映射 {vnetconnid:serveid}
var localconnids_sm_ = util.NewSyncMap()             // map[int]struct{} // 已连接 local connid列表 {connid:{}}
var localconnmap_sm_ = util.NewSyncMap()             // map[int]net.Conn // 已连接 local connid映射 {connid:net.conn}
var localconnidtoserveidmap_sm_ = util.NewSyncMap()  // map[int]string // connid(仅只local端connid)与serveid的对应关系映射 {connid:serveid}

// serves和use维护

var serveidmap_sm_ = util.NewSyncMap()   // map[string]util.Serve // 已申请serveid的serve映射，{serveid:util.Serve}
var servenoidlist = []util.Serve{}       // []util.Serve{} // 未申请serveid的serve列表，[util.Serve]
var serveregmap_sm_ = util.NewSyncMap()  // map[string]string // 已注册的serve映射，{serveid:serveaddr}
var useidmap_sm_ = util.NewSyncMap()     // map[string]util.Use // use中有serveid的映射，{serveid:util.Use}
var uselistenmap_sm_ = util.NewSyncMap() // map[string]net.Listener // use开启的listen映射，{serveid:net.Listener}
var routeserves_sm_ = util.NewSyncMap()  // map[string]map[string]util.Serve // route端返回的全量的serve，{vnetid:{serveid:util.Serve}}
var routevnets_sm_ = util.NewSyncMap()   // map[string]string // route端返回的全量的vnet列表，{vnetid:vnetinfo}

// unserve维护

var unservemap_local_sm_ = util.NewSyncMap()  // map[string]map[string]string // 请求的非注册服务，{vnetid:{unserveaddr:serveid}}
var unservemap_remote_sm_ = util.NewSyncMap() // map[string]string // 被请求的非注册服务，{serveid:unserveaddr}
var unservetimer_sm_ = util.NewSyncMap()      // map[string]time.Timer // unserve定时器，定时清除未服务的 unservemap_remote

// 几个控制标

var heartflag = true      // 暂时中断心跳发送，测ping时暂停心跳
var hearttime int64 = 0   // 最新heart时间，测ping时使用
var servestime int64 = 0  // 最新routeserves时间，终端使用
var vnetstime int64 = 0   // 最新vnetsinfo时间，终端使用
var unservetime int64 = 0 // 最新unserve时间，终端使用
var unserveflag = true    // unserve处理进程，因可能存在多个unserve请求，所以需要排队
var autoconfig = true     // 暂时中断自动检测配置
var terminalflag = false  // 暂时中断终端命令显示时使用

// 全局管道

var rouchan = make(chan []byte)               // local -> route
var locchan = make(chan []byte)               // local <- route
var cnfchan = make(chan util.LocalMessage, 5) // terminal -> config，local -> config

// 本地

var vnetid = util.Msgvnetidempty      // 本端vnet标识
var routeconn net.Conn = nil          // 与route端的连接，断线重连
const timeout = 100                   // timeout等待循环次数 // 10s
const timeinterval = time.Second / 10 // timeout一次100ms

var lastroutemsgtime = time.Now().UnixNano() // routemsg的最新接收时间，用来做超时判断
func updateroutemsgtime() {
	lastroutemsgtime = time.Now().UnixNano()
}

type VnetConfig struct {
	Routeaddr util.RouteAddr
	Vnetinfo  util.VnetInfo
	Servelist util.ServeList
	Uselist   util.UseList
}

var vnetconfig VnetConfig // 代替util.Servelist和util.Uselist的本地缓存，用以和重读的config做比较

func init() {
	if len(os.Args) >= 2 {
		util.Configfile = os.Args[len(os.Args)-1]
	}
	fmt.Println("reading", util.Configfile, "...")
	if !util.Loadconfig() {
		fmt.Println(util.Configfile, "has err")
		os.Exit(0)
	}
	vnetconfig = VnetConfig{
		Routeaddr: util.Routeaddr,
		Vnetinfo:  util.Vnetinfo,
		Servelist: util.Servelist,
		Uselist:   util.Uselist,
	}
}

func main() {
	fmt.Println(time.Now().Format("[2006-01-02 15:04:05]"), "vnet start ...")
	// 连接 route
	connect_to_route() // 若route不在线，则每隔3s进行重连
	defer close(rouchan)
	defer close(locchan)
	defer close(cnfchan)
	go route_readmsg_handle()
	go vnet_message_handle()
	go vnet_config_handle()
	// 阻塞等待rouchan，route端write
	for roumsg := range rouchan {
		_, err := routeconn.Write(roumsg)
		if err != nil {
			// fmt.Println("route write err:", err, time.Now().UnixNano()/1000000-1661416000000)
		}
	}
}

// 读取route的消息，整理数据帧
func route_readmsg_handle() {
	var routeisdisconnectedfmt_bool = true // 控制显示断链信息
	// 接受消息
	var messagecache []byte
	var messagelen int = -1
	for {
		var buffer [util.Buffersize]byte
		bufsize, err := routeconn.Read(buffer[:])
		if err != nil {
			// 执行断线重连
			if routeisdisconnectedfmt_bool {
				fmt.Println(time.Now().Format("[2006-01-02 15:04:05]"), "route is disconnected, retrying ...")
				routeisdisconnectedfmt_bool = false
				// robotgo.KeyTap("enter") // 模拟敲击一次回车，以过度terminal的显示
			}
			time.Sleep(timeinterval * 30) // 3s
			connect_to_route() // 尝试重连
			continue
		}
		routeisdisconnectedfmt_bool = true
		// 刷新最新route消息时间，用来做超时判断
		updateroutemsgtime()
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
				// 发送缓存----------------------------------------------------------------
				locchan <- messagecache[:messagelen]                          // 发送到本地服务local，不去掉头部长度信息
				messagecache = append([]byte{}, messagecache[messagelen:]...) // 更新缓存
				messagelen = -1                                               // 重置msglen
			} else { // 缓存长度<msglen，等待下一次msg
				break
			}
		}
	}
	os.Exit(0)
}

// 处理route的消息，转发，配置，控制，关闭，创建remote
func vnet_message_handle() {
	// route消息处理 // 检查控制码
	for msg := range locchan {
		if localmsg, ok := util.Parsemsg_local(msg); ok {
			switch localmsg.Ctrl {
			case util.Ctrlcloseconn: // 关闭一个ventconn
				{
					// 应当判断是remote端消息还是local端消息，如果是local端，则connid=connid，如果是remote端消息，则connid=vnetconnid
					if vnetid == localmsg.Vnetid {
						// local
						if localconn, ok := localconnmap_sm_.Load(localmsg.Connid); ok {
							localconn.(net.Conn).Close()                                        // 可以关闭vnetread
							localconnmap_sm_.Delete(localmsg.Connid)                            // 可以关闭vnetwrite
							if _, ok := localconnidtoserveidmap_sm_.Load(localmsg.Connid); ok { // localmsg.Serveid是不准确的，需要重新获取
								localconnidtoserveidmap_sm_.Delete(localmsg.Connid) // 维护链路，如果是unserve的serveid，则需要进一步处理
							}
							if localmsg.Serveid != util.Msgserveidempty { // serveid="00"证明时route发过来的，就不用再回发了
								rouchan <- util.Addmsglen([]byte(localmsg.Vnetconnid + util.Ctrlcloseconn + util.Msgserveidempty)) // 可以关闭route
							}
							go func() { // 延迟几秒内不得重复这个id，然后再删除
								time.Sleep(timeinterval * 100) // 10s
								localconnids_sm_.Delete(localmsg.Connid)
							}()
						} else {
							// fmt.Println("non connection! Ctrlcloseconn local connid:", localmsg.Connid)
						}
					} else {
						// remote
						if remoteconn, ok := remoteconnmap_sm_.Load(localmsg.Vnetconnid); ok {
							remoteconn.(net.Conn).Close()                                                  // 可以关闭remoteread
							remoteconnmap_sm_.Delete(localmsg.Vnetconnid)                                  // 可以关闭remotewrite
							if serveid, ok := remoteconnidtoserveidmap_sm_.Load(localmsg.Vnetconnid); ok { // localmsg.Serveid是不准确的，需要重新获取
								remoteconnidtoserveidmap_sm_.Delete(localmsg.Vnetconnid) // 维护链路，如果是unserve的serveid，则需要进一步处理
								go remoteunservecheck(serveid.(string))                  // unserve检查
							}
							if localmsg.Serveid != util.Msgserveidempty { // serveid="00"证明时route发过来的，就不用再回发了
								rouchan <- util.Addmsglen([]byte(localmsg.Vnetconnid + util.Ctrlcloseconn + util.Msgserveidempty)) // 可以关闭route
							}
							// 延迟几秒内不得重复这个id，然后再删除 // remote端应该不需要延迟
							remoteconnids_sm_.Delete(localmsg.Vnetconnid)
						} else {
							// fmt.Println("non connection! Ctrlcloseconn remote vnetconnid:", localmsg.Vnetconnid)
						}
					}
				}
			case util.Ctrlsendmsg: // 传递消息
				{
					// 以下判断应当更改为判断serveid，或者当vnetid==00时做处理 ---------------
					// 应当判断是remote端消息还是local端消息，如果是local端，则connid=connid，如果是remote端消息，则connid=vnetconnid
					if vnetid == localmsg.Vnetid {
						// local
						if localconn, ok := localconnmap_sm_.Load(localmsg.Connid); ok {
							_, err := localconn.(net.Conn).Write(localmsg.Msg)
							if err != nil {
								// fmt.Println("Ctrlsendmsg local write err:", err)
								localconn.(net.Conn).Close()
								go func(localmsg util.LocalMessage) { // 避免卡死 // locchan满的时候，当扇出阻塞，扇入就会被卡，此时使用goroutine避免，但是可能会乱序
									locchan <- util.Addmsglen(localmsg.Vnetconnid + util.Ctrlcloseconn + localmsg.Serveid) // 发送结束消息
								}(localmsg)
							}
						} else {
							// fmt.Println("non connection! Ctrlsendmsg local connid:", localmsg.Connid)
						}
					} else {
						// remote
						if !remote_dial_handel(localmsg) { // 尝试创建新的remote连接 // 需要确认serveid，再查找serveid对应的serve信息，进行remote连接建立
							break
						}
						if remoteconn, ok := remoteconnmap_sm_.Load(localmsg.Vnetconnid); ok {
							_, err := remoteconn.(net.Conn).Write(localmsg.Msg)
							if err != nil {
								remoteconn.(net.Conn).Close()
								go func(localmsg util.LocalMessage) { // 避免卡死 // locchan满的时候，当扇出阻塞，扇入就会被卡，此时使用goroutine避免，但是可能会乱序
									locchan <- util.Addmsglen(localmsg.Vnetconnid + util.Ctrlcloseconn + localmsg.Serveid) // 发送结束消息
								}(localmsg)
							}
						} else {
							// fmt.Println("non connection! Ctrlsendmsg remote vnetconnid:", localmsg.Vnetconnid)
						}
					}
				}
			case util.Ctrlsetvnetid, util.Ctrlgetserveid, util.Ctrlregserveid, util.Ctrldelserveid, util.Ctrlgetserves, util.Ctrlheartbeat, util.Ctrlsendmsg_noserve, util.Ctrlvnetinfo, util.Ctrlgetvnetsinfo:
				{
					// 控制类的消息，转发至cnfchan配置中心
					go func(localmsg util.LocalMessage) {
						cnfchan <- localmsg
					}(localmsg)
				}
			default:
				{
					// 不一般
					fmt.Println("switch localmsg.Ctrl err default")
				}
			}
		} else {
			fmt.Println("Parsemsg_local localmsg err", string(msg))
		}
	}
}

// 处理本地配置，本地serve，映射use，配置操作
func vnet_config_handle() {
	// 心跳 // 定时请求
	go func() {
		for {
			// 间隔10s
			time.Sleep(timeinterval * 60) // 6s
			if heartflag {
				rouchan <- util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrlheartbeat + util.Msgserveidempty)
			}
		}
	}()
	// autoconfog循环检测，对配置的改变做自动化响应 // 对servelist和uselist进行处理 // 申请serveid，注册serve，开启uselisten，更新vnetinfo
	go func() {
		// 整理出原配置中已有serveid的serve
		for _, unserve := range vnetconfig.Servelist {
			if unserve.Serveid != util.Msgserveidempty { // 可以考虑是否加入对serveid范围的限制，必须 [70,99]
				if _, ok := serveidmap_sm_.Load(unserve.Serveid); !ok {
					serveidmap_sm_.Store(unserve.Serveid, unserve) // 加入已申请serveid列表
				}
			} else {
				servenoidlist = append(servenoidlist, unserve) // 加入无serveid列表
			}
		}
		// 整理出原配置中已有serveid的use
		for _, unuse := range vnetconfig.Uselist {
			if unuse.Serveid != util.Msgserveidempty {
				if _, ok := useidmap_sm_.Load(unuse.Serveid); !ok {
					useidmap_sm_.Store(unuse.Serveid, unuse)
				}
			}
		}
		// autoconfog // 循环检测，自动化响应
		for {
			if !autoconfig {
				time.Sleep(timeinterval) // 0.1s
				continue
			}
			// 将已有serveid的serve进行注册
			for _, kv := range serveidmap_sm_.KVlist() {
				serveid, serve := kv[0].(string), kv[1].(util.Serve)
				if serve.Active { // 未激活的忽略
					if _, ok := serveregmap_sm_.Load(serveid); !ok { // 忽略已注册的serveid
						serve.Serveid = serveid // 改变serve的serveid
						servejson, err := json.Marshal(serve)
						if err != nil {
							fmt.Println("json.Marshal serve err", err)
							continue
						}
						roumsg := append([]byte(vnetid+util.Msgconnidempty+util.Ctrlregserveid+serveid), servejson...)
						rouchan <- util.Addmsglen(roumsg)
					}
				}
			}
			// 为没有serveid的serve申请serveid
			for _, serve := range servenoidlist {
				if serve.Active { // 未激活的忽略
					rouchan <- util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrlgetserveid + util.Msgserveidempty)
				}
			}
			// 单独检查是否在routeserves中 // 应延迟几秒
			for _, serveid := range serveregmap_sm_.Keys() {
				serveid := serveid.(string)
				// 先检查serveregmap中的serve是否还在routeserves中，不在则删掉
				if serves, ok := routeserves_sm_.Load(vnetid); ok { // 因为routerserves的数据结构，需要先找vnet
					serves := serves.(map[string]util.Serve)
					if _, ok := serves[serveid]; !ok { // 不在route提供的serves中
						locchan <- util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrldelserveid + serveid) // 给自己发一个注销成功的假消息
					} else {
						// 删掉serveregmap中多出的serveid,向route发送delserveid消息
						if serve, ok := serveidmap_sm_.Load(serveid); !ok || !serve.(util.Serve).Active {
							rouchan <- util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrldelserveid + serveid)
						}
					}
				} else { // 不在routeserves中
					locchan <- util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrldelserveid + serveid) // 给自己发一个注销成功的假消息
				}
			}
			// 间隔 1s
			time.Sleep(timeinterval * 10) // 1s
			// 验证use.serveid在route的routeserves中，以及没有在uselistenmap中
			for _, kv := range useidmap_sm_.KVlist() {
				serveid, use := kv[0].(string), kv[1].(util.Use)
				if use.Active { // 未激活的忽略
					var serveidflag = false                       // 标记这个serveid是否还在routeserves中
					for _, kv := range routeserves_sm_.KVlist() { // 因为routerserves的数据结构，需要先循环vnet
						tovnetid, serves := kv[0].(string), kv[1].(map[string]util.Serve)
						if tovnetid != vnetid { // 不是本地vnet
							if serve, ok := serves[serveid]; ok { // 在route提供的serves中
								serveidflag = true
								if _, ok := uselistenmap_sm_.Load(serveid); !ok { // 不在已映射use中
									// 开启这个use映射
									if len(strings.ReplaceAll(use.Port, " ", "")) == 0 { // 如果use.port为空，则使用对应serve的port
										use.Port = serve.Port
										useidmap_sm_.Store(serveid, use)
									}
									go local_listen_handel(use) // 开启成功加入uselistenmap // local_listen_read()中实现 // 可以外部控制关闭listen
								}
								break
							}
						}
					}
					if !serveidflag {
						// 不在服务区 // 自动关闭 // 仅关闭listen // 如果active=false的话，则在恢复服务时无法自动开启
						if uselisten, ok := uselistenmap_sm_.Load(serveid); ok { // 在已映射use中
							uselisten.(net.Listener).Close()
							uselistenmap_sm_.Delete(serveid)
						}
					}
				}
			}
			// 检查uselistenmap中的serveid是否还在useidmap中，或者是否在useidmap中被设置active=false，则关闭listen删除uselistenmap的serveid
			for _, kv := range uselistenmap_sm_.KVlist() {
				serveid, uselisten := kv[0].(string), kv[1].(net.Listener)
				if use, ok := useidmap_sm_.Load(serveid); !ok || !use.(util.Use).Active {
					uselisten.Close()
					uselistenmap_sm_.Delete(serveid)
				}
			}
			// 检查vnetinfo是否改变 // 目前仅是更新 name，后面可以扩展
			if vnetconfig.Vnetinfo.Name != util.Vnetinfo.Name || vnetconfig.Vnetinfo.Group != util.Vnetinfo.Group {
				// 更新vnetinfo
				vnetconfig.Vnetinfo = util.Vnetinfo
				// 发送vnetinfo
				var vnetinfo = map[string]string{"name": vnetconfig.Vnetinfo.Name, "group": vnetconfig.Vnetinfo.Group}
				vnetinfojson, err := json.Marshal(vnetinfo)
				if err != nil {
					fmt.Println("json.Marshal vnetinfo err", err)
				} else {
					rouchan <- util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrlvnetinfo + util.Msgserveidempty + string(vnetinfojson))
				}
			}
			// 间隔 1s
			time.Sleep(timeinterval * 20) // 2s
		}
	}()
	// 开启terminal
	if vnetconfig.Vnetinfo.Terminal {
		go vnet_terminal_handle()
	}
	// 阻塞等待相关配置消息的操作
	for cnfmsg := range cnfchan {
		switch cnfmsg.Ctrl {
		case util.Ctrlsetvnetid:
			{
				if cnfmsg.Vnetid != util.Msgvnetidempty {
					vnetid = cnfmsg.Vnetid
				} else {
					// 失败重发
					rouchan <- util.Addmsglen(util.Msgvnetidempty + util.Msgconnidempty + util.Ctrlsetvnetid + util.Msgserveidempty)
				}
			}
		case util.Ctrlgetserveid:
			{
				// 得到一个serveid // 拿出一个noserveid，加入有serveid
				if cnfmsg.Serveid != util.Msgserveidempty {
					if len(servenoidlist) > 0 {
						if _, ok := serveidmap_sm_.Load(cnfmsg.Serveid); !ok { // 已被添加
							if len(servenoidlist) > 0 {
								serve := servenoidlist[0]
								if strings.HasPrefix(serve.Name, "unserve_"+vnetid+"_") {
									serve.Name = "unserve_" + vnetid + "_" + cnfmsg.Serveid
								}
								serveidmap_sm_.Store(cnfmsg.Serveid, serve)
								if len(servenoidlist) <= 1 {
									servenoidlist = []util.Serve{}
								} else {
									servenoidlist = append(servenoidlist[0:0], servenoidlist[1:]...)
								}
							}
						}
					}
				}
			}
		case util.Ctrlregserveid:
			{
				// 获取注册结果，注册成功则将其加入serveregmap
				if cnfmsg.Serveid != util.Msgserveidempty {
					if _, ok := serveregmap_sm_.Load(cnfmsg.Serveid); !ok {
						if serve, ok := serveidmap_sm_.Load(cnfmsg.Serveid); ok {
							serve := serve.(util.Serve)
							serveregmap_sm_.Store(cnfmsg.Serveid, serve.Ip+":"+serve.Port)
							if strings.HasPrefix(serve.Name, "unserve_"+vnetid+"_") { // serve.Name == "unserve_"+vnetid+"_"+cnfmsg.Serveid
								unservemap_remote_sm_.Store(cnfmsg.Serveid, serve.Ip+":"+serve.Port) // 加入未注册服务映射
								remoteunservecheck(cnfmsg.Serveid)                                   // unserve检查
							}
						}
					}
				} else {
					// 注册失败则删除其在serveidmap中的serveid，并将其退回到servenoidlist中
					serveid := string(cnfmsg.Msg) // 根据协议，msg中会写有注册失败的serveid
					if serve, ok := serveidmap_sm_.Load(serveid); ok {
						serve := serve.(util.Serve)
						id, err := strconv.ParseInt(serveid, 10, 0)
						if err != nil {
							fmt.Println("strconv.ParseInt serveid err", err)
						} else {
							if id <= int64(util.Serveslimit) {
								serve.Serveid = util.Msgserveidempty // "00"
								servenoidlist = append(servenoidlist, serve)
								serveidmap_sm_.Delete(serveid)
							} else {
								// active置false让这个serve不再注册 // 这会在掉线重连后无法注册，所以不再置false
							}
						}
					}
				}
			}
		case util.Ctrldelserveid:
			{
				// 获取删除结果，删除成功则将其从serveregmap中删除
				if cnfmsg.Serveid != util.Msgserveidempty {
					serveregmap_sm_.Delete(cnfmsg.Serveid)
					if _, ok := unservemap_remote_sm_.Load(cnfmsg.Serveid); ok {
						// 需要清除 unserve_remote
						unservemap_remote_sm_.Delete(cnfmsg.Serveid)
						// 清除掉注册进serveidmap的serveid
						serveidmap_sm_.Delete(cnfmsg.Serveid)
					}
					// 需要清理这个serveid下的所有connid
					closeremoteserveidconns(cnfmsg.Serveid)
				} else {
					fmt.Println("Ctrldelserveid serve", string(cnfmsg.Msg), "err")
				}
			}
		case util.Ctrlgetserves:
			{
				if len(cnfmsg.Msg) > 0 {
					var routeserves_ = make(map[string]map[string]util.Serve)
					if strings.ReplaceAll(string(cnfmsg.Msg), " ", "") != "{}" {
						err := json.Unmarshal(cnfmsg.Msg, &routeserves_)
						if err != nil {
							fmt.Println("Ctrlgetserves json.Unmarshal routeserves err", err)
						}
					}
					routeserves_sm_.From_string_map_string_serve(routeserves_)
					servestime = time.Now().UnixNano()
				} else {
					fmt.Println("Ctrlgetserves get routeserves err")
					// 重发
					rouchan <- util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrlgetserves + util.Msgserveidempty)
				}
			}
		case util.Ctrlheartbeat:
			{
				// 更新心跳时间
				hearttime = time.Now().UnixNano()
			}
		case util.Ctrlsendmsg_noserve:
			{
				// 请求访问未注册服务
				var unserve_req = map[string]string{"serveid": util.Msgserveidempty}
				err := json.Unmarshal(cnfmsg.Msg, &unserve_req)
				if err != nil {
					fmt.Println("json.Unmarshal unserve err", err)
					break
				}
				if unserve_req["unserve"] == "request" { // 有新请求 // 时间较长，应放入goroutine中
					go func(tovnetid string, unserve_req map[string]string) {
						for i := 0; i < timeout; i++ {
							if unserveflag { // 同步操作
								break
							}
							time.Sleep(timeinterval) // 0.1s
						}
						if !unserveflag {
							fmt.Println("等待超时，有其他unserve任务忙 ...")
							return
						}
						unserveflag = false
						// 处理 ...
						var unserve_resp = map[string]string{"unserve": "response", "serveaddr": unserve_req["ip"] + ":" + unserve_req["port"], "serveid": util.Msgserveidempty}
						if vnetconfig.Vnetinfo.Allow_unserve {
							if !vnetconfig.Vnetinfo.Unserve_remote && !(unserve_req["ip"] == "127.0.0.1" || unserve_req["ip"] == "localhost") { // 不允许remote，也不是localhost或127.0.0.1
								fmt.Println("unserve remoteip be not allowed")
							} else if unservemap_remote_sm_.Len() < vnetconfig.Vnetinfo.Unserve_num { // 超过数量限制
								// 注册服务 // 添加servenoidlist // 先检查本地serveregmap，有无ip:port一致的，有则直接返回，无则添加servenoidlist
								for _, kv := range serveregmap_sm_.KVlist() {
									serveid, serveaddr := kv[0].(string), kv[1].(string)
									if serveaddr == unserve_req["ip"]+":"+unserve_req["port"] { // 已注册返回serveid // 未注册则直接加入servenoidlist
										unserve_resp["serveid"] = serveid
										break
									}
								}
								if unserve_resp["serveid"] == util.Msgserveidempty { // 未注册，加入servenoidlist
									var newunserve = util.Serve{
										Serveid: util.Msgserveidempty,
										Type:    "tcp",
										Ip:      unserve_req["ip"],
										Port:    unserve_req["port"],
										Name:    "unserve_" + vnetid + "_" + strconv.Itoa(unservemap_remote_sm_.Len()+1),
										Info:   "unserve_" + strconv.Itoa(int(time.Now().Unix())),
										Active: true,
									}
									servenoidlist = append(servenoidlist, newunserve)
									flagtime := time.Now().UnixNano()
									for i := 0; i < timeout; i++ {
										time.Sleep(timeinterval) // 0.1s
										if servestime >= flagtime {
											break
										}
									}
									if servestime >= flagtime {
										// 收到routeserves，检测是否有这个地址 // 直接检查 serveregmap
										for _, kv := range serveregmap_sm_.KVlist() {
											serveid, serveaddr := kv[0].(string), kv[1].(string)
											if serveaddr == newunserve.Ip+":"+newunserve.Port { // && newunserve.Name == "unserve_" + vnetid + "_" + serveid
												unserve_resp["serveid"] = serveid
											}
										}
									} else {
										fmt.Println("unserve申请serveid及注册失败")
									}
								}
							}
						}
						// 发送
						unserve_respjson, err := json.Marshal(unserve_resp)
						if err != nil {
							fmt.Println("json.Marshal unserve_resp err", err)
							unserveflag = true
							return
						}
						rouchan <- util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrlsendmsg_noserve + "0" + tovnetid + string(unserve_respjson)) // serveid改后比vnetid多一位
						unserveflag = true                                                                                                             // 解除
					}(cnfmsg.Vnetid, unserve_req)
				} else if unserve_req["unserve"] == "response" { // 是响应
					// 检查unserve的serveid
					if unserve_req["serveid"] != util.Msgserveidempty { // 成功 // 失败就不说了
						if unservelocal, ok := unservemap_local_sm_.Load(cnfmsg.Vnetid); ok { // 是否有vnetid
							unservelocal := unservelocal.(map[string]string)
							if serveid, ok := unservelocal[unserve_req["serveaddr"]]; ok { // 是否有serveaddr
								if serveid == util.Msgserveidempty { // 是否有serveid
									unservelocal[unserve_req["serveaddr"]] = unserve_req["serveid"] // 增加unserve记录
									unservemap_local_sm_.Store(cnfmsg.Vnetid, unservelocal)
								} else { // 原来就有serveid，则这个地址早被开启过一次
									// 不变，使用原serveid，以免原来的服务被断 // 还是检查下吧
									if _, ok := uselistenmap_sm_.Load(serveid); ok {
										// 还有服务，不变
									} else {
										unservelocal[unserve_req["serveaddr"]] = unserve_req["serveid"] // 增加unserve记录
										unservemap_local_sm_.Store(cnfmsg.Vnetid, unservelocal)
									}
								}
							}
						}
					}
					// 计时
					unservetime = time.Now().UnixNano()
				}
			}
		case util.Ctrlvnetinfo:
			{
				if len(cnfmsg.Msg) > 0 {
					// 失败重发
					// 发送 vnetinfo
					var vnetinfo = map[string]string{"name": vnetconfig.Vnetinfo.Name, "group": vnetconfig.Vnetinfo.Group}
					vnetinfojson, err := json.Marshal(vnetinfo)
					if err != nil {
						fmt.Println("json.Marshal vnetinfo err", err)
					} else {
						rouchan <- util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrlvnetinfo + util.Msgserveidempty + string(vnetinfojson))
					}
				}
			}
		case util.Ctrlgetvnetsinfo:
			{
				if len(cnfmsg.Msg) > 0 {
					var routevnets_ = map[string]string{}
					err := json.Unmarshal(cnfmsg.Msg, &routevnets_)
					if err != nil {
						fmt.Println("Ctrlgetvnetsinfo json.Unmarshal routevnets err", err)
						break
					}
					routevnets_sm_.From_string_string(routevnets_)
					vnetstime = time.Now().UnixNano()
				} else {
					fmt.Println("Ctrlgetvnetsinfo get routevnets err")
					// 重发
					rouchan <- util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrlgetvnetsinfo + util.Msgserveidempty)
				}
			}
		default:
			{
				// 不一般
				fmt.Println("switch cnfmsg.Ctrl err default")
			}
		}
	}
}

// 终端服务，按需开启
func vnet_terminal_handle() {
	// 开局暂不开启
	for {
		if terminalflag {
			time.Sleep(timeinterval * 20) // 2s
			break
		} else {
			time.Sleep(timeinterval) // 0.1s
			continue
		}
	}
	// 新建/开启/暂停/删除 serve // 新建/开启/暂停/删除 use // 查看route端serves // 查看route端vnets // 查看本地端serve // 查看本地端use // 测试与route的ping延迟 // 请求未注册服务
	fmt.Println("\n使用命令: [help] [reload] [serve] [use] [show] [ping] [unserve] [exit]")
	// 读取命令行
	var reader = bufio.NewReader(os.Stdin)
	for {
		if !terminalflag {
			time.Sleep(timeinterval) // 0.1s
			continue
		}
		fmt.Print("\n>: ")
		ter, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("err:", err)
			continue
		}
		fmt.Print("\n")
		var terminal = strings.Fields(ter) // 未输入时长度是1
		if len(terminal) < 1 {
			continue
		}
		switch strings.ToLower(terminal[0]) {
		case "help":
			{
				fmt.Println("reload  重新加载配置，可指定配置文件")
				fmt.Println("serve   本地服务操作，创建、执行、开始、暂停、删除等")
				fmt.Println("use     映射服务操作，创建、执行、开始、暂停、删除等")
				fmt.Println("show    显示信息，本端vnet、本端serve、本端use、route端serves、route端vnets等")
				fmt.Println("ping    测试与route端的回传延迟")
				fmt.Println("unserve 请求非注册服务")
				fmt.Println("exit    退出本端vnet")
			}
		case "reload": // 重读配置 // 尽量维持连接不断
			{
				// 重读配置
				if len(terminal) >= 2 { // 有新配置文件
					if strings.HasSuffix(terminal[len(terminal)-1], ".ini") {
						var oldfilename = util.Configfile
						util.Configfile = terminal[len(terminal)-1]
						if !util.Loadconfig() {
							fmt.Println("reload", util.Configfile, "has err")
							util.Configfile = oldfilename
							break
						}
					} else {
						fmt.Println("reload", terminal[len(terminal)-1], "is not *.ini file")
						break
					}
				} else {
					util.Configfile = util.Defaultconfigfile
					if !util.Loadconfig() {
						fmt.Println("reload", util.Configfile, "has err")
						break
					}
				}
				autoconfig = false
				// 重新配置 serve
				var reserveidmap = map[string]util.Serve{} // 临时变量，serveidmap
				var reservenoidlist = []util.Serve{}       // 临时变量，servenoidlist
				for _, unserve := range util.Servelist { // 整理出新配置中已有serveid的serve
					if unserve.Serveid != util.Msgserveidempty { // 可以考虑是否加入对serveid范围的限制，必须 [70,99]
						if _, ok := reserveidmap[unserve.Serveid]; !ok {
							reserveidmap[unserve.Serveid] = unserve // 加入已申请serveid列表
						}
					} else {
						reservenoidlist = append(reservenoidlist, unserve) // 加入无serveid列表
					}
				}
				for _, kv := range serveregmap_sm_.KVlist() { // 检查serveregmap，serveid相同的对比ip:port决定这个regserve保留或注销
					serveid, serveaddr := kv[0].(string), kv[1].(string)
					if serve, ok := reserveidmap[serveid]; ok {
						if serveaddr == serve.Ip+":"+serve.Port { // ip:port相同的保留，不同的注销
						} else {
							go func() {
								time.Sleep(timeinterval / 10)                                                           // 10ms
								rouchan <- util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrldelserveid + serveid) // 注销这个注册服务
							}()
						}
						continue
					}
				}
				var delnoidindex = []int{}
				for i, serve := range reservenoidlist { // 检查reservenoidlist中的ip:port是否已在serveregmap中，有则继承serveid并放入serveidmap中
					for _, kv := range serveregmap_sm_.KVlist() {
						serveid, serveaddr := kv[0].(string), kv[1].(string)
						if serveaddr == serve.Ip+":"+serve.Port { // 有在regmap中的
							if _, ok := reserveidmap[serveid]; ok { // serveid已在reserveidmap中
								continue // 已在前一步检查serveregmap时排除了这一情况，当serveid相同时，ip:port肯定相同  // 应删除这个noidserve？autoconfig中删除？serve不删除，允许重复
							} else { // serveid不在reserveidmap中，将这个serveid添加到reserveidmap中
								id, err := strconv.Atoi(serveid)
								if err != nil { // 不是数字
									fmt.Println("strconv.Atoi serveid er", err)
								} else {
									if id > util.Serveslimit { // 不能继承
										break
									}
								}
								reserveidmap[serveid] = serve // 继承id
								delnoidindex = append(delnoidindex, i)
								break
							}
						}
					}
				}
				for j := len(delnoidindex) - 1; j >= 0; j-- { // 删除reservenoidlist中的已转移到reserveidmap中的serve
					var i = delnoidindex[j]
					appendreservenoidlist := []util.Serve{}
					if i+1 < len(reservenoidlist) {
						appendreservenoidlist = servenoidlist[i+1:]
					}
					reservenoidlist = append(reservenoidlist[:i], appendreservenoidlist...) // 删除noid中的serve
				}
				// 重新配置 use
				var reuseidmap = map[string]util.Use{} // 临时变量，useidmap
				for _, unuse := range util.Uselist {   // 整理出原配置中已有serveid的use
					if unuse.Serveid != util.Msgserveidempty {
						if _, ok := reuseidmap[unuse.Serveid]; !ok {
							reuseidmap[unuse.Serveid] = unuse
						}
					}
				}
				for _, kv := range useidmap_sm_.KVlist() { // serveid是否一样，不一样的不用管，一样的看映射地址ip:port是否一致，不一致就关listen
					serveid, use := kv[0].(string), kv[1].(util.Use)
					if reuse, ok := reuseidmap[serveid]; ok {
						if use.Ip == reuse.Ip && use.Port == reuse.Port {
							// 一毛一样的保留
						} else {
							// 不一样的，关闭原listen
							if listen, ok := uselistenmap_sm_.Load(serveid); ok { // 在已映射use中
								listen.(net.Listener).Close()
								uselistenmap_sm_.Delete(serveid)
							}
						}
					}
				}
				vnetconfig.Servelist = util.Servelist
				vnetconfig.Uselist = util.Uselist
				serveidmap_sm_.From_string_serve(reserveidmap)
				servenoidlist = reservenoidlist
				useidmap_sm_.From_string_use(reuseidmap)
				autoconfig = true
			}
		case "serve": // serve相关
			{
				if len(terminal) >= 2 {
					// new新建 open开启 close关闭 delete删除 list列表serves show列表本地
					switch strings.ToLower(terminal[1]) {
					case "new":
						{
							// active=false，serveid=util.Msgserveidempty
							var newserve = util.Serve{
								Serveid: util.Msgserveidempty,
								Type:    "tcp",
								Ip:      "127.0.0.1",
								Port:    "",
								Name:    "",
								Info:    "custom",
								Active:  false,
							}
							fmt.Print("    name: ")
							name, err := reader.ReadString('\n')
							if err != nil {
								fmt.Println("new name err:", err)
								break
							}
							name = strings.ReplaceAll(name, " ", "")
							name = strings.ReplaceAll(name, "\r", "")
							name = strings.ReplaceAll(name, "\n", "")
							fmt.Print("    type(tcp): ")
							type_, err := reader.ReadString('\n')
							if err != nil {
								fmt.Println("new type err:", err)
								break
							}
							type_ = strings.ReplaceAll(type_, " ", "")
							type_ = strings.ReplaceAll(type_, "\r", "")
							type_ = strings.ReplaceAll(type_, "\n", "")
							fmt.Print("    ip(127.0.0.1): ")
							ip, err := reader.ReadString('\n')
							if err != nil {
								fmt.Println("new ip err:", err)
								break
							}
							ip = strings.ReplaceAll(ip, " ", "")
							ip = strings.ReplaceAll(ip, "\r", "")
							ip = strings.ReplaceAll(ip, "\n", "")
							fmt.Print("    port: ")
							port, err := reader.ReadString('\n')
							if err != nil {
								fmt.Println("new port err:", err)
								break
							}
							port = strings.ReplaceAll(port, " ", "")
							port = strings.ReplaceAll(port, "\r", "")
							port = strings.ReplaceAll(port, "\n", "")
							newserve.Name = name
							if type_ != "" {
								newserve.Type = type_
							}
							if ip != "" {
								newserve.Ip = ip
							}
							p, err := strconv.Atoi(port)
							if err != nil {
								fmt.Println("输入的port:", port, "有误...")
								break
							} else {
								if p <= 0 {
									fmt.Println("int(port) must be > 0", port)
									break
								}
							}
							newserve.Port = port
							fmt.Println("newserve", newserve)
							// 添加
							servenoidlist = append(servenoidlist, newserve)
						}
					case "run":
						{
							// active=true，serveid=util.Msgserveidempty
							var newserve = util.Serve{
								Serveid: util.Msgserveidempty,
								Type:    "tcp",
								Ip:      "127.0.0.1",
								Port:    "",
								Name:    "",
								Info:    "custom",
								Active:  true,
							}
							fmt.Print("    name: ")
							name, err := reader.ReadString('\n')
							if err != nil {
								fmt.Println("create name err:", err)
								break
							}
							name = strings.ReplaceAll(name, " ", "")
							name = strings.ReplaceAll(name, "\r", "")
							name = strings.ReplaceAll(name, "\n", "")
							fmt.Print("    type(tcp): ")
							type_, err := reader.ReadString('\n')
							if err != nil {
								fmt.Println("create type err:", err)
								break
							}
							type_ = strings.ReplaceAll(type_, " ", "")
							type_ = strings.ReplaceAll(type_, "\r", "")
							type_ = strings.ReplaceAll(type_, "\n", "")
							fmt.Print("    ip(127.0.0.1): ")
							ip, err := reader.ReadString('\n')
							if err != nil {
								fmt.Println("create ip err:", err)
								break
							}
							ip = strings.ReplaceAll(ip, " ", "")
							ip = strings.ReplaceAll(ip, "\r", "")
							ip = strings.ReplaceAll(ip, "\n", "")
							fmt.Print("    port: ")
							port, err := reader.ReadString('\n')
							if err != nil {
								fmt.Println("create port err:", err)
								break
							}
							port = strings.ReplaceAll(port, " ", "")
							port = strings.ReplaceAll(port, "\r", "")
							port = strings.ReplaceAll(port, "\n", "")
							newserve.Name = name
							if type_ != "" {
								newserve.Type = type_
							}
							if ip != "" {
								newserve.Ip = ip
							}
							p, err := strconv.Atoi(port)
							if err != nil {
								fmt.Println("输入的port:", port, "有误...")
								break
							} else {
								if p <= 0 {
									fmt.Println("int(port) must be > 0", port)
									break
								}
							}
							newserve.Port = port
							fmt.Println("newserve", newserve)
							// 添加
							servenoidlist = append(servenoidlist, newserve)
						}
					case "open":
						{
							// active=true
							if len(terminal) >= 3 {
								serveid := terminal[2]
								if serve, ok := serveidmap_sm_.Load(serveid); ok {
									serve := serve.(util.Serve)
									serve.Active = true
									serveidmap_sm_.Store(serveid, serve) // 不知道需不需要 // 需要
								} else {
									for i, serve := range servenoidlist {
										if serve.Name == serveid {
											go func() {
												appendservenoidlist := []util.Serve{}
												if i+1 < len(servenoidlist) {
													appendservenoidlist = servenoidlist[i+1:]
												}
												serve.Active = true
												servenoidlist = append(servenoidlist[:i], appendservenoidlist...)
												servenoidlist = append(servenoidlist, serve)
											}()
											break
										}
									}
								}
							}
						}
					case "close":
						{
							// active=false
							if len(terminal) >= 3 {
								serveid := terminal[2]
								if serve, ok := serveidmap_sm_.Load(serveid); ok {
									serve := serve.(util.Serve)
									serve.Active = false
									id, err := strconv.Atoi(serve.Serveid)
									if err != nil { // 有可能不是数字
										fmt.Println("strconv.Atoi serveid err", err)
										serveidmap_sm_.Store(serveid, serve) // 不知道需不需要 // 需要
									} else {
										if id > util.Serveslimit {
											serveidmap_sm_.Store(serveid, serve) // 不知道需不需要 // 需要
										} else {
											serve.Serveid = util.Msgserveidempty
											servenoidlist = append(servenoidlist, serve)
											serveidmap_sm_.Delete(serveid)
										}
									}
								}
							}
						}
					case "delete":
						{
							// delserveid
							if len(terminal) >= 3 {
								serveid := terminal[2]
								if _, ok := serveidmap_sm_.Load(serveid); ok {
									serveidmap_sm_.Delete(serveid) // 删除这个serve
								} else {
									for i, serve := range servenoidlist {
										if serve.Name == serveid {
											go func() {
												appendservenoidlist := []util.Serve{}
												if i+1 < len(servenoidlist) {
													appendservenoidlist = servenoidlist[i+1:]
												}
												servenoidlist = append(servenoidlist[:i], appendservenoidlist...)
											}()
											break
										}
									}
								}
							}
						}
					default:
						{
							fmt.Println("serve [new] [open] [run] [close] [delete]")
						}
					}
				} else {
					fmt.Println("serve [new] [open] [run] [close] [delete]")
				}
			}
		case "use": // use相关
			{
				if len(terminal) >= 2 {
					// new新建 open开启 close关闭 delete删除 list列表serves show列表本地
					switch strings.ToLower(terminal[1]) {
					case "new":
						{
							var newuse = util.Use{
								Serveid: util.Msgserveidempty,
								Ip:      "127.0.0.1",
								Port:    "",
								Active:  false,
							}
							fmt.Print("    serveid: ")
							serveid, err := reader.ReadString('\n')
							if err != nil {
								fmt.Println("new name err:", err)
								break
							}
							serveid = strings.ReplaceAll(serveid, " ", "")
							serveid = strings.ReplaceAll(serveid, "\r", "")
							serveid = strings.ReplaceAll(serveid, "\n", "")
							if _, ok := useidmap_sm_.Load(serveid); ok {
								fmt.Println("serveid already exist")
								break
							}
							if serveid != "" { // 检测serveid
								newuse.Serveid = serveid
							} else {
								fmt.Println("serveid must be non-empty")
								break
							}
							fmt.Print("    ip(127.0.0.1): ")
							ip, err := reader.ReadString('\n')
							if err != nil {
								fmt.Println("new ip err:", err)
								break
							}
							ip = strings.ReplaceAll(ip, " ", "")
							ip = strings.ReplaceAll(ip, "\r", "")
							ip = strings.ReplaceAll(ip, "\n", "")
							fmt.Print("    port: ")
							port, err := reader.ReadString('\n')
							if err != nil {
								fmt.Println("new port err:", err)
								break
							}
							port = strings.ReplaceAll(port, " ", "")
							port = strings.ReplaceAll(port, "\r", "")
							port = strings.ReplaceAll(port, "\n", "")
							if ip != "" {
								newuse.Ip = ip
							}
							p, err_ := strconv.Atoi(port)
							if err_ != nil {
								fmt.Println("输入的port:", port, "有误...")
								break
							} else {
								if p <= 0 {
									fmt.Println("int(port) must be > 0", port)
									break
								}
							}
							newuse.Port = port
							fmt.Println("newuse", newuse)
							// 添加
							useidmap_sm_.Store(serveid, newuse)
						}
					case "run":
						{
							var newuse = util.Use{
								Serveid: util.Msgserveidempty,
								Ip:      "127.0.0.1",
								Port:    "",
								Active:  true,
							}
							fmt.Print("    serveid: ")
							serveid, err := reader.ReadString('\n')
							if err != nil {
								fmt.Println("new name err:", err)
								break
							}
							serveid = strings.ReplaceAll(serveid, " ", "")
							serveid = strings.ReplaceAll(serveid, "\r", "")
							serveid = strings.ReplaceAll(serveid, "\n", "")
							if _, ok := useidmap_sm_.Load(serveid); ok {
								fmt.Println("serveid already exist")
								break
							}
							if serveid != "" { // 检测serveid
								newuse.Serveid = serveid
							} else {
								fmt.Println("serveid must be non-empty")
								break
							}
							fmt.Print("    ip(127.0.0.1): ")
							ip, err := reader.ReadString('\n')
							if err != nil {
								fmt.Println("new ip err:", err)
								break
							}
							ip = strings.ReplaceAll(ip, " ", "")
							ip = strings.ReplaceAll(ip, "\r", "")
							ip = strings.ReplaceAll(ip, "\n", "")
							fmt.Print("    port: ")
							port, err := reader.ReadString('\n')
							if err != nil {
								fmt.Println("new port err:", err)
								break
							}
							port = strings.ReplaceAll(port, " ", "")
							port = strings.ReplaceAll(port, "\r", "")
							port = strings.ReplaceAll(port, "\n", "")
							if ip != "" {
								newuse.Ip = ip
							}
							p, err := strconv.Atoi(port)
							if err != nil {
								fmt.Println("输入的port:", port, "有误...")
								break
							} else {
								if p <= 0 {
									fmt.Println("int(port) must be > 0", port)
									break
								}
							}
							newuse.Port = port
							fmt.Println("newuse", newuse)
							// 添加
							if _, ok := useidmap_sm_.Load(serveid); ok {
								fmt.Println("serveid already exist")
								break
							}
							useidmap_sm_.Store(serveid, newuse)
						}
					case "open":
						{
							if len(terminal) >= 3 {
								serveid := terminal[2]
								if use, ok := useidmap_sm_.Load(serveid); ok {
									use := use.(util.Use)
									use.Active = true
									useidmap_sm_.Store(serveid, use) // 不知道需不需要 // 需要
								}
							}
						}
					case "close":
						{
							if len(terminal) >= 3 {
								serveid := terminal[2]
								if use, ok := useidmap_sm_.Load(serveid); ok {
									use := use.(util.Use)
									use.Active = false
									useidmap_sm_.Store(serveid, use) // 不知道需不需要 // 需要
								}
							}
						}
					case "delete":
						{
							if len(terminal) >= 3 {
								serveid := terminal[2]
								useidmap_sm_.Delete(serveid)
							}
						}
					default:
						{
							fmt.Println("use [new] [open] [run] [close] [delete]")
						}
					}
				} else {
					fmt.Println("use [new] [open] [run] [close] [delete]")
				}
			}
		case "show": // 查询相关 // 本地serve 本地use routeserves routevnets
			{
				if len(terminal) >= 2 {
					switch strings.ToLower(terminal[1]) {
					case "serve":
						{
							fmt.Println("本终端已注册", serveregmap_sm_.Len(), "个服务:")
							fmt.Println("serveid\tname\ttype\tip\tport\tinfo\tactive\tis_reg")
							for _, kv := range serveidmap_sm_.KVlist() {
								serveid, serve := kv[0].(string), kv[1].(util.Serve)
								var info = serveid + "\t" + serve.Name + "\t" + serve.Type + "\t" + serve.Ip + "\t" + serve.Port + "\t" + serve.Info + "\t" + strconv.FormatBool(serve.Active)
								if _, ok := serveregmap_sm_.Load(serveid); ok {
									info = info + "\t" + "TRUE"
								} else {
									info = info + "\t" + "FALSE"
								}
								fmt.Println(info)
							}
							for _, serve := range servenoidlist {
								var info = serve.Serveid + "\t" + serve.Name + "\t" + serve.Type + "\t" + serve.Ip + "\t" + serve.Port + "\t" + serve.Info + "\t" + strconv.FormatBool(serve.Active)
								info = info + "\t" + "FALSE"
								fmt.Println(info)
							}
							fmt.Println("- 共", serveidmap_sm_.Len()+len(servenoidlist), "个 -")
						}
					case "use":
						{
							fmt.Println("本终端已开启", uselistenmap_sm_.Len(), "个映射:")
							fmt.Println("serveid\tip\tport\tactive\tlisten\tname\tvnet\ttype\t_ip_\t_port_\tinfo")
							for _, kv := range useidmap_sm_.KVlist() {
								serveid, use := kv[0].(string), kv[1].(util.Use)
								var info = serveid + "\t" + use.Ip + "\t" + use.Port + "\t" + strconv.FormatBool(use.Active)
								if _, ok := uselistenmap_sm_.Load(serveid); ok {
									info = info + "\t" + "TRUE"
								} else {
									info = info + "\t" + "FALSE"
								}
								for _, kv := range routeserves_sm_.KVlist() {
									vnetid, serves := kv[0].(string), kv[1].(map[string]util.Serve)
									if serve, ok := serves[serveid]; ok {
										info = info + "\t" + serve.Name + "\t" + vnetid + "\t" + serve.Type + "\t" + serve.Ip + "\t" + serve.Port + "\t" + serve.Info
									}
								}
								fmt.Println(info)
							}
							fmt.Println("- 共", useidmap_sm_.Len(), "个 -")
						}
					case "vnet":
						{
							fmt.Println("vnetid:\t\t\t\t", vnetid)
							fmt.Println("vnet名称:\t\t\t", vnetconfig.Vnetinfo.Name)
							fmt.Println("vnet分组:\t\t\t", vnetconfig.Vnetinfo.Group)
							fmt.Println("开启命令行:\t\t\t", vnetconfig.Vnetinfo.Terminal)
							fmt.Println("自启动远程服务列表:\t\t", vnetconfig.Vnetinfo.Startserve)
							fmt.Println("自启动本地映射列表:\t\t", vnetconfig.Vnetinfo.Startuse)
							fmt.Println("允许访问未注册服务:\t\t", vnetconfig.Vnetinfo.Allow_unserve)
							fmt.Println("允许访问非本地未注册服务:\t", vnetconfig.Vnetinfo.Unserve_remote)
							fmt.Println("允许开启的未注册服务数量:\t", vnetconfig.Vnetinfo.Unserve_num)
						}
					case "serves":
						{
							fmt.Println("请求route的服务列表 ...")
							// 开始时间
							var flagtime = time.Now().UnixNano()
							// 发送getserves
							rouchan <- util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrlgetserves + util.Msgserveidempty)
							for i := 0; i < timeout; i++ {
								time.Sleep(timeinterval) // 0.1s
								// 检测
								if servestime >= flagtime {
									break
								}
							}
							if servestime >= flagtime {
								fmt.Println("route服务列表:")
							} else {
								fmt.Println("请求失败，显示", (time.Now().UnixNano()-servestime)/1e6, "ms 前的route服务列表:")
							}
							fmt.Println("serveid\tvnet\tname\ttype\tip\tport\tinfo")
							var servesum = 0
							for _, kv := range routeserves_sm_.KVlist() {
								vnetid, serves := kv[0].(string), kv[1].(map[string]util.Serve)
								for serveid, serve := range serves {
									var info = serveid + "\t" + vnetid + "\t" + serve.Name + "\t" + serve.Type + "\t" + serve.Ip + "\t" + serve.Port + "\t" + serve.Info
									fmt.Println(info)
									servesum += 1
								}
							}
							fmt.Println("- ", routeserves_sm_.Len(), "个vnet终端, 共注册", servesum, "个服务 -")
						}
					case "vnets":
						{
							fmt.Println("请求route的vnet列表 ...")
							// 开始时间
							var flagtime = time.Now().UnixNano()
							// 发送getvnetsinfo
							rouchan <- util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrlgetvnetsinfo + util.Msgserveidempty)
							for i := 0; i < timeout; i++ {
								time.Sleep(timeinterval) // 0.1s
								// 检测
								if vnetstime >= flagtime {
									break
								}
							}
							if vnetstime >= flagtime {
								fmt.Println("vnet列表:")
							} else {
								fmt.Println("请求失败，显示", (time.Now().UnixNano()-servestime)/1e6, "ms 前的vnet列表:")
							}
							fmt.Println("vnetid\tinfo")
							for _, kv := range routevnets_sm_.KVlist() {
								vnetid, vnet := kv[0].(string), kv[1].(string)
								var info = vnetid + "\t" + vnet
								fmt.Println(info)
							}
							fmt.Println("- 共", routevnets_sm_.Len(), "个vnet终端 -")
						}
					default:
						{
							fmt.Println("show [serve] [use] [vnet] [serves] [vnets]")
						}
					}
				} else {
					fmt.Println("show [serve] [use] [vnet] [serves] [vnets]")
				}
			}
		case "ping": // 延迟检测
			{
				fmt.Println("测试与route的回传延时 ...")
				heartflag = false // 暂停心跳
				for i := 1; i <= 3; i++ {
					time.Sleep(timeinterval * 10) // 1s
					// 开始时间
					var flagtime = time.Now().UnixNano()
					// 发送heartbeat
					rouchan <- util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrlheartbeat + util.Msgserveidempty)
					// 检测时间是否更新
					for j := 0; j < timeout/2; j++ {
						time.Sleep(timeinterval) // 0.1s
						// 检测
						if hearttime >= flagtime {
							break
						}
					}
					// 输出
					deltatime := (hearttime - flagtime)
					unitname := "ns"
					if deltatime >= 0 {
						if deltatime/1e3 > 0 {
							deltatime = deltatime / 1e3
							unitname = "μs"
							if deltatime/1e3 > 0 {
								deltatime = deltatime / 1e3
								unitname = "ms"
							}
						}
						fmt.Println("第", i, "次ping时间:", deltatime, unitname)
					} else {
						fmt.Println("第", i, "次ping err", hearttime, flagtime)
					}
				}
				heartflag = true // 恢复心跳
			}
		case "unserve": // 请求未注册服务
			{
				var unserve = map[string]string{"unserve": "request"}
				fmt.Print("    to vnetid: ")
				tovnetid, err := reader.ReadString('\n')
				if err != nil {
					fmt.Println("unserve vnetid err:", err)
					break
				}
				tovnetid = strings.ReplaceAll(tovnetid, " ", "")
				tovnetid = strings.ReplaceAll(tovnetid, "\r", "")
				tovnetid = strings.ReplaceAll(tovnetid, "\n", "")
				if _, ok := routeserves_sm_.Load(tovnetid); ok && vnetid != tovnetid {
					fmt.Print("    ip(127.0.0.1): ")
					ip, err := reader.ReadString('\n')
					if err != nil {
						fmt.Println("unserve ip err:", err)
						break
					}
					ip = strings.ReplaceAll(ip, " ", "")
					ip = strings.ReplaceAll(ip, "\r", "")
					ip = strings.ReplaceAll(ip, "\n", "")
					fmt.Print("    port: ")
					port, err := reader.ReadString('\n')
					if err != nil {
						fmt.Println("unserve port err:", err)
						break
					}
					port = strings.ReplaceAll(port, " ", "")
					port = strings.ReplaceAll(port, "\r", "")
					port = strings.ReplaceAll(port, "\n", "")
					if ip != "" {
						unserve["ip"] = ip
					} else {
						unserve["ip"] = "127.0.0.1"
					}
					if port != "" {
						unserve["port"] = port
					} else {
						fmt.Println("unserve port must non-empty")
						break
					}
					unserveaddr := unserve["ip"] + ":" + unserve["port"]
					if unservelocal, ok := unservemap_local_sm_.Load(tovnetid); ok {
						unservelocal := unservelocal.(map[string]string)
						if unserveid, ok := unservelocal[unserveaddr]; ok {
							if _, ok := uselistenmap_sm_.Load(unserveid); ok {
								fmt.Println("你需要开启的服务已提供并在监听中 -- serveid:", unserveid)
								break
							}
						}
						unservelocal[unserveaddr] = util.Msgserveidempty
						unservemap_local_sm_.Store(tovnetid, unservelocal)
					}
					unservemap_local_sm_.Store(tovnetid, map[string]string{unserveaddr: util.Msgserveidempty}) // 创建 unservemap_local
					fmt.Println("unservemap_local", unservemap_local_sm_.KVlist())
					unservejson, err := json.Marshal(unserve)
					if err != nil {
						fmt.Println("json.Marshal unserve err", err)
						break
					}
					for i := 0; i < timeout; i++ {
						if unserveflag { // 同步操作
							break
						}
						time.Sleep(timeinterval) // 0.1s
					}
					if !unserveflag {
						fmt.Println("等待超时，有其他unserve任务忙 ...")
						break
					}
					unserveflag = false
					var flagtime = time.Now().UnixNano()
					rouchan <- util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrlsendmsg_noserve + "0" + tovnetid + string(unservejson)) // serveid改为比vnetid多一位
					fmt.Println("已向 vnetid:", tovnetid, "的vnet终端请求其未注册的", unserveaddr, "服务，等待回应 ...")
					fmt.Println("注意：unserve服务保质期为 3min，请尽快食用 ...")
					for i := 0; i < timeout; i++ {
						if unservetime > flagtime {
							break
						}
						time.Sleep(timeinterval) // 0.1s
					}
					if unservetime > flagtime {
						if tovnetunserve, ok := unservemap_local_sm_.Load(tovnetid); ok {
							if serveid, ok := tovnetunserve.(map[string]string)[unserveaddr]; ok {
								if serveid != util.Msgserveidempty {
									fmt.Println("成功获取到 vnetid:", tovnetid, "的vnet终端的服务 - 已注册serveid:", serveid, "继续开启 ...\n")
									var newunserveuse = util.Use{
										Serveid: serveid,
										Ip:      "127.0.0.1",
										Port:    "",
										Active:  true,
									}
									fmt.Print("    ip(127.0.0.1): ")
									ip, err := reader.ReadString('\n')
									if err != nil {
										fmt.Println("new ip err:", err)
										unserveflag = true // 解除
										break
									}
									ip = strings.ReplaceAll(ip, " ", "")
									ip = strings.ReplaceAll(ip, "\r", "")
									ip = strings.ReplaceAll(ip, "\n", "")
									fmt.Print("    port: ")
									port, err := reader.ReadString('\n')
									if err != nil {
										fmt.Println("new port err:", err)
										unserveflag = true // 解除
										break
									}
									port = strings.ReplaceAll(port, " ", "")
									port = strings.ReplaceAll(port, "\r", "")
									port = strings.ReplaceAll(port, "\n", "")
									if ip != "" {
										newunserveuse.Ip = ip
									}
									newunserveuse.Port = port
									fmt.Println("newunserveuse", newunserveuse)
									// 添加
									if _, ok := useidmap_sm_.Load(serveid); ok {
										fmt.Println("serveid:", serveid, "already exist in uselist, check with [show use] and open")
									} else {
										useidmap_sm_.Store(serveid, newunserveuse)
										time.Sleep(timeinterval * 10) // 1s
									}
									unserveflag = true // 解除
									break
								}
							}
						}
					}
					fmt.Println("开启unserve服务失败！但是仍有可能在稍后自行开启，请输入[show serves]核查 ...")
					unserveflag = true // 解除
				} else {
					fmt.Println("vnetid 输入有误", tovnetid, "不存在") //, routeserves)
				}
			}
		case "exit": // 退出
			{
				fmt.Println("exit ...")
				os.Exit(0)
			}
		default:
			{
				fmt.Println("使用命令: [help] [reload] [serve] [use] [show] [ping] [unserve] [exit]")
			}
		}
	}
}

// 监听一个use，并accept，有connect则更新localconnmap，localconnids，启动一个这个conn的read协程 // 更新uselistenmap，供外部关闭
func local_listen_handel(use util.Use) {
	if _, ok := uselistenmap_sm_.Load(use.Serveid); ok { // 如果已存在，则返回
		return
	}
	locallisten, err := net.Listen("tcp", use.Ip+":"+use.Port)
	if err != nil {
		return
	}
	defer locallisten.Close()
	defer uselistenmap_sm_.Delete(use.Serveid)
	uselistenmap_sm_.Store(use.Serveid, locallisten) // 开启成功加入uselistenmap
	for {
		localconn, err := locallisten.Accept()
		if err != nil {
			// 需要关闭这个serveid下的所有conn
			closelocalserveidconns(use.Serveid)
			break
		}
		if id, ok := util.Generateid_sm_(localconnids_sm_.Keys(), util.Connslimit); ok {
			localconnmap_sm_.Store(id, localconn)
			localconnids_sm_.Store(id, struct{}{})
			localconnidtoserveidmap_sm_.Store(id, use.Serveid)
			go local_readmsg_handle(use.Serveid, id, localconn)
		} else {
			localconn.Close() // 满了的话直接关闭
		}
	}
}

// 连接serve，建立一个conn，更新remoteconnmap，remoteconnids，启动一个这个conn的read协程 // 如果成功或已存在返回true，建立失败返回false
func remote_dial_handel(localmsg util.LocalMessage) bool {
	if _, ok := remoteconnmap_sm_.Load(localmsg.Vnetconnid); !ok {
		// 查找serveid对应的已注册serve，获取serveaddr
		if serveaddr, ok := serveregmap_sm_.Load(localmsg.Serveid); ok {
			// 连接
			remoteconn, err := net.Dial("tcp", serveaddr.(string))
			if err != nil {
				return false
			}
			remoteconnids_sm_.Store(localmsg.Vnetconnid, struct{}{})
			remoteconnmap_sm_.Store(localmsg.Vnetconnid, remoteconn)
			remoteconnidtoserveidmap_sm_.Store(localmsg.Vnetconnid, localmsg.Serveid) // 维护链路，如果是unserve的serveid，则需要进一步处理
			remoteunservecheck(localmsg.Serveid)                                      // unserve检查
			go remote_readmsg_handle(localmsg.Serveid, localmsg.Vnetconnid, remoteconn)
		} else {
			fmt.Println("remote serveid unregistered", localmsg.Serveid, serveregmap_sm_.KVlist())
			return false
		}
	}
	return true
}

// 接受一个localconn，阻塞read
func local_readmsg_handle(serveid string, id int, localconn net.Conn) {
	defer localconn.Close()
	connid := util.Fixinttostr(id, util.Msgconnidsize)
	for {
		var buffer [util.Buffersize - util.Msglen_toserveidsize]byte
		bufsize, err := localconn.Read(buffer[:])
		if err != nil {
			locchan <- util.Addmsglen(vnetid + connid + util.Ctrlcloseconn + serveid) // 发送结束消息
			break
		}
		roumsg := append([]byte(vnetid+connid+util.Ctrlsendmsg+serveid), buffer[:bufsize]...)
		rouchan <- util.Addmsglen(roumsg)
	}
}

// 接受一个remoteconn，阻塞read
func remote_readmsg_handle(serveid string, vnetconnid string, remoteconn net.Conn) {
	defer remoteconn.Close()
	for {
		var buffer [util.Buffersize - util.Msglen_toserveidsize]byte
		bufsize, err := remoteconn.Read(buffer[:])
		if err != nil {
			locchan <- util.Addmsglen([]byte(vnetconnid + util.Ctrlcloseconn + serveid)) // 发送结束消息
			break
		}
		roumsg := append([]byte(vnetconnid+util.Ctrlsendmsg+serveid), buffer[:bufsize]...)
		rouchan <- util.Addmsglen(roumsg)
	}
}

// unserve定时检查，当unserve的所有链路被清除时，定时 10s 后将serveid注销 // 过于复杂了
func remoteunservecheck(serveid string) {
	if _, ok := unservemap_remote_sm_.Load(serveid); !ok {
		return
	}
	if timer, ok := unservetimer_sm_.Load(serveid); ok {
		timer := timer.(time.Timer)
		// 检查 remoteconnidtoserveidmap 中是否还有这个serveid
		var flag = false
		for _, serveid_ := range remoteconnidtoserveidmap_sm_.Values() {
			if serveid == serveid_.(string) {
				flag = true
				break
			}
		}
		if flag { // 有
			timer.Reset(timeinterval * 3000) // 5min
		} else { // 没有
			timer.Reset(timeinterval * 600) // 60s
		}
	} else {
		// 设置新的timer // 第一次设置30s
		unservetimer_sm_.Store(serveid, *time.NewTimer(timeinterval * 600 * 3)) // 60s // 180s
		go func(serveid string) {
			for {
				if timer, ok := unservetimer_sm_.Load(serveid); ok {
					timer := timer.(time.Timer)
					<-timer.C
					var flag = false
					for _, serveid_ := range remoteconnidtoserveidmap_sm_.Values() {
						if serveid == serveid_.(string) {
							flag = true
							break
						}
					}
					if flag { // 有 // 重置
						unservetimer_sm_.Store(serveid, *time.NewTimer(timeinterval * 3000)) // 5min
					} else { // 没有 // 清理
						timer.Stop()
						unservetimer_sm_.Delete(serveid)
						if _, ok := serveregmap_sm_.Load(serveid); ok {
							// 注销这个服务
							rouchan <- util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrldelserveid + serveid)
						}
						break
					}
				}
			}
		}(serveid)
	}
}

// 连接route // 若route不在线，则每隔3s进行重连 // 在断链后进行重连
func connect_to_route() bool {
	// 先确定发送 vnetinfo 的数据正确，否则退出
	var vnetinfo = map[string]string{"name": vnetconfig.Vnetinfo.Name, "group": vnetconfig.Vnetinfo.Group}
	vnetinfojson, err := json.Marshal(vnetinfo)
	if err != nil {
		fmt.Println("json.Marshal vnetinfo err", err)
		fmt.Println("will exit later ...")
		os.Exit(0)
	}
	terminalflag = false         // 暂停 terminal
	vnetid = util.Msgvnetidempty // 重置
	routeserves_sm_.Clear()      // 重置 routeserves，重连后 vnetid 会变
	fmt.Println(time.Now().Format("[2006-01-02 15:04:05]"), "connecting to route ...")
	var routeconn_ net.Conn = nil
	// 连接 // 一直尝试重连，每隔3s
	for {
		conn, err := net.Dial("tcp", vnetconfig.Routeaddr.Ip+":"+vnetconfig.Routeaddr.Port)
		if err != nil {
			time.Sleep(timeinterval * 30) // 3s
			continue
		}
		routeconn_ = conn
		break
	}
	routeconn = routeconn_ // 更新routeconn
	fmt.Println(time.Now().Format("[2006-01-02 15:04:05]"), "route is connected ...")
	// 发送必要配置消息
	go func(routeconn_ net.Conn, vnetinfojson []byte) {
		var err error = nil
		// 请求vnetid // 确定vnetid成功获取到，否则关闭连接
		var failtimes = 10 // 尝试次数
		for {
			// 请求vnetid
			_, err = routeconn_.Write(util.Addmsglen(util.Msgvnetidempty + util.Msgconnidempty + util.Ctrlsetvnetid + util.Msgserveidempty))
			if err != nil {
				routeconn_.Close()
				return
			}
			time.Sleep(timeinterval * 10) // 1s
			// 确定vnetid成功获取到，否则关闭连接
			if vnetid == util.Msgvnetidempty {
				if failtimes < 0 {
					routeconn_.Close()
					return
				}
				failtimes--
				continue
			} else {
				break
			}
		}
		fmt.Println("\nget vnetid:", vnetid, "\n")
		// 发送 vnetinfo
		_, err = routeconn_.Write(util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrlvnetinfo + util.Msgserveidempty + string(vnetinfojson)))
		if err != nil {
			routeconn_.Close()
			return
		}
		// 请求一次route端的全量routeserves // 由route端广播了
		_, err = routeconn_.Write(util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrlgetserves + util.Msgserveidempty))
		if err != nil {
			routeconn_.Close()
			return
		}
		// 发送一次心跳
		_, err = routeconn_.Write(util.Addmsglen(vnetid + util.Msgconnidempty + util.Ctrlheartbeat + util.Msgserveidempty))
		if err != nil {
			routeconn_.Close()
			return
		}
		// 开启 terminal
		terminalflag = true
	}(routeconn_, vnetinfojson)
	// 给命令行填充一个 >: // 没啥必要
	if vnetconfig.Vnetinfo.Terminal {
		go func() {
			time.Sleep(timeinterval * 20) // 2s
			fmt.Print("\n>: ")
		}()
	}
	return true
}

// 主动关闭一个serveid下的所有conn
func closeremoteserveidconns(serveid string) {
	for _, kv := range remoteconnidtoserveidmap_sm_.KVlist() {
		vnetconnid, serveid_ := kv[0].(string), kv[1].(string)
		if serveid_ == serveid {
			locchan <- util.Addmsglen(vnetconnid + util.Ctrlcloseconn + serveid) // 给自己发关闭这个conn的消息
		}
	}
}

// 主动关闭一个serveid下的所有conn
func closelocalserveidconns(serveid string) {
	for _, kv := range localconnidtoserveidmap_sm_.KVlist() {
		connid, serveid_ := kv[0].(int), kv[1].(string)
		if serveid_ == serveid {
			locchan <- util.Addmsglen(vnetid + util.Fixinttostr(connid, util.Msgconnidsize) + util.Ctrlcloseconn + serveid) // 给自己发关闭这个conn的消息
		}
	}
}
