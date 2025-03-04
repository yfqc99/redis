package main

import (
	"bufio"
	"chatting2/utils"
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type UserManager struct {
	users map[string]*User
	mu    sync.Mutex
}

type User struct {
	name string
	conn net.Conn
	mu   sync.Mutex
}

type RedisManager struct {
	client *redis.Client
	ctx    context.Context
}

type ChatManager struct {
	userManager  *UserManager
	redisManager *RedisManager
}

var file *os.File

func main() {
	fmt.Println("等待连接")

	// 初始化聊天管理器
	chatManager := setupChatManager()

	file = utils.InitLog("G:/goProject/src/chatting2-v7/document/record.txt")
	defer file.Close()

	// 监听连接
	listen, err := net.Listen("tcp", "0.0.0.0:8888")
	if err != nil {
		fmt.Println("网络连接错误", err)
		return
	}
	defer listen.Close()

	// 服务器关闭检测
	ctx, cancel := context.WithCancel(context.Background())
	go handleServerShutdown(ctx, chatManager.userManager, listen, cancel)

	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Println("服务器断开")
			continue
		}
		cancel()
		// 有新连接
		go chatManager.HandleConnection(conn)
	}
}

func setupChatManager() *ChatManager {
	userManager := &UserManager{
		users: make(map[string]*User),
	}

	// 初始化 Redis
	redisClient, ctx := newRedisClient("192.168.157.129:6379")
	redisManager := &RedisManager{
		client: redisClient,
		ctx:    ctx,
	}

	redisClient.FlushAll(ctx)

	// 初始化聊天管理器
	chatManager := &ChatManager{
		userManager:  userManager,
		redisManager: redisManager,
	}

	// 启动消息广播协程
	go chatManager.BroadcastMessages()

	return chatManager
}

func newRedisClient(addr string) (*redis.Client, context.Context) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})
	return client, ctx
}

func handleServerShutdown(ctx context.Context, userManager *UserManager, listener net.Listener, cancel context.CancelFunc) {
	for {
		// 检查是否有用户在线
		userManager.mu.Lock()
		userCount := len(userManager.users)
		userManager.mu.Unlock()
		if userCount == 0 {
			timer := time.NewTimer(10 * time.Second)
			// 所有用户已退出，启动关闭计时器
			//fmt.Println("没有用户登录，等待10秒关闭服务器...")
			select {
			case <-timer.C:
				if len(userManager.users) == 0 {
					// 10秒后关闭服务器
					fmt.Println("关闭服务器...")
					listener.Close()
					os.Exit(0)
				} else {
					timer.Stop()
				}
			case <-ctx.Done():
				// 上下文被取消，重置定时器
				timer.Stop()
			}
		} else {
			ctx, cancel = context.WithCancel(context.Background())
		}
		// 等待一段时间后重新检查
		time.Sleep(1 * time.Second)
	}
}

func (cm *ChatManager) HandleConnection(conn net.Conn) {
	user := &User{
		conn: conn,
	}

	// 获取用户信息
	if err := cm.RegisterUser(user); err != nil {
		return
	}

	// 接收消息
	cm.ReceiveMessages(user)
}

func (cm *ChatManager) RegisterUser(user *User) error {
	for {
		reader := bufio.NewReader(user.conn)
		str1, _ := utils.Decode(reader)
		msg := strings.Trim(str1, "\r\n")
		var receive utils.Data
		if err := json.Unmarshal([]byte(msg), &receive); err != nil {
			return err
		}

		user.name = receive.Message
		// 检查用户名是否已存在
		if cm.userManager.IsUserExist(user.name) {
			send := utils.Data{
				TypeId:  2,
				Message: "用户名重复，请重新登录",
			}
			if err := cm.SendData(user, send); err != nil {
				return err
			}
		} else {
			// 用户名有效，注册用户
			send := utils.Data{
				TypeId:  1,
				Message: "欢迎 " + user.name + " 进入聊天室",
			}
			if err := cm.SendData(user, send); err != nil {
				return err
			}
			cm.userManager.AddUser(user)
			cm.redisManager.AddZsorted(user.name)
			// 广播用户登录消息
			go cm.BroadcastUserJoined(user.name, send.Message)
			break
		}
	}
	return nil
}

func (cm *ChatManager) ReceiveMessages(user *User) {
	defer user.conn.Close()

	for {
		reader := bufio.NewReader(user.conn)
		str1, err := utils.Decode(reader)
		if err != nil {
			// 用户断开连接
			cm.userManager.RemoveUser(user.name)
			cm.redisManager.RemoveZsorted(user.name)
			cm.redisManager.client.XAdd(
				cm.redisManager.ctx,
				utils.Stream("chat", map[string]interface{}{
					"typeId":  2,
					"name":    user.name,
					"message": "已下线",
				}),
			)
			return
		}

		msg := strings.Trim(str1, "\r\n")
		var receive utils.Data
		if err := json.Unmarshal([]byte(msg), &receive); err != nil {
			continue
		}

		user.mu.Lock()
		cm.redisManager.client.ZIncrBy(cm.redisManager.ctx, "rank", 1, user.name)
		user.mu.Unlock()

		// 将消息发送到 Redis Stream
		cm.redisManager.client.XAdd(
			cm.redisManager.ctx,
			utils.Stream("chat", map[string]interface{}{
				"typeId":  receive.TypeId,
				"name":    user.name,
				"message": receive.Message,
			}),
		)
		msg = time.Now().String() + " " + user.name + "：" + receive.Message
		fmt.Println(msg)
		utils.Record(file, msg)
	}
}

func (cm *ChatManager) BroadcastMessages() {
	stream := "chat"
	group := "group"
	cm.redisManager.client.XGroupCreateMkStream(cm.redisManager.ctx, stream, group, "$")

	for {
		result, err := cm.redisManager.client.XReadGroup(
			cm.redisManager.ctx,
			&redis.XReadGroupArgs{
				Group:    group,
				Consumer: "consumer",
				Streams:  []string{stream, ">"},
				Count:    1,
				Block:    0,
			},
		).Result()
		if err != nil {
			if err != redis.Nil {
				fmt.Println("超时发送：", err)
			}
			continue
		}

		if len(result) == 0 {
			continue
		}

		for _, msg := range result[0].Messages {
			id, _ := strconv.Atoi(msg.Values["typeId"].(string))
			name := msg.Values["name"].(string)
			message := msg.Values["message"].(string)

			cm.Broadcast(id, name, message)
			cm.redisManager.client.XAck(cm.redisManager.ctx, stream, group, msg.ID)
		}
	}
}

func (cm *ChatManager) Broadcast(id int, name, message string) {
	var send utils.Data
	users := cm.userManager.GetAllUsers()

	for _, user := range users {
		if user.name == name {
			// 用户自己的消息
			switch id {
			case 0:
				// 用户退出
				send.TypeId = 0
				send.Message = name + "：" + message
				cm.userManager.RemoveUser(name)
				cm.redisManager.RemoveZsorted(name)
				if err := cm.SendData(user, send); err != nil {
					continue
				}
			case 1:
				// 查看排行榜
				send.TypeId = 2
				send.Message = message + "\n" + strings.Join(cm.GetRank(), "\n")
				if err := cm.SendData(user, send); err != nil {
					continue
				}
			}
			continue
		} else {
			send.TypeId = 2
			send.Message = name + "：" + message
			if id == 1 || id == 0 {
				continue
			}
			if id == 3 {
				send.Message = message
			}
		}
		if err := cm.SendData(user, send); err != nil {
			continue
		}
	}
}

func (cm *ChatManager) SendData(user *User, data utils.Data) error {
	jsonByte, err := json.Marshal(data)
	if err != nil {
		return err
	}
	str, err := utils.Encode(string(jsonByte))
	if err != nil {
		return err
	}
	user.conn.Write(str)
	return nil
}

func (cm *ChatManager) BroadcastUserJoined(name, message string) {
	// 广播用户登录消息
	cm.redisManager.client.XAdd(
		cm.redisManager.ctx,
		utils.Stream("chat", map[string]interface{}{
			"typeId":  3,
			"name":    name,
			"message": message,
		}),
	)
	msg := time.Now().String() + " " + message
	fmt.Println(msg)
	utils.Record(file, msg)
}

func (cm *ChatManager) GetRank() []string {
	res, err := cm.redisManager.client.ZRevRange(cm.redisManager.ctx, "rank", 0, -1).Result()
	if err != nil {
		fmt.Println("获取排行失败", err)
		return nil
	}

	rank := make([]string, len(res))
	for i, r := range res {
		rank[i] = fmt.Sprintf("%d\t%s", i+1, r)
	}
	return rank
}

func (cm *RedisManager) RemoveZsorted(name string) {
	cm.client.ZRem(cm.ctx, "rank", name)
}

func (cm *RedisManager) AddZsorted(name string) {
	cm.client.ZAdd(cm.ctx, "rank", &redis.Z{
		Score:  0,
		Member: name,
	})
}

// UserManger methods
func (um *UserManager) IsUserExist(name string) bool {
	_, exists := um.users[name]
	return exists
}

func (um *UserManager) AddUser(user *User) {
	um.mu.Lock()
	defer um.mu.Unlock()
	um.users[user.name] = user
}

func (um *UserManager) RemoveUser(name string) {
	um.mu.Lock()
	defer um.mu.Unlock()
	delete(um.users, name)
}

func (um *UserManager) GetAllUsers() []*User {
	um.mu.Lock()
	defer um.mu.Unlock()
	users := make([]*User, 0, len(um.users))
	for _, user := range um.users {
		users = append(users, user)
	}
	return users
}
