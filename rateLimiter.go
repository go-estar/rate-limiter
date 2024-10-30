package rateLimiter

import (
	"context"
	stderrors "errors"
	"github.com/go-estar/config"
	"github.com/go-estar/redis"
	"github.com/thoas/go-funk"
	"strings"
	"time"
)

var (
	ErrorBlock           = stderrors.New("forbidden")
	ErrorWhiteListExists = stderrors.New("whiteList exists")
	ErrorBlockListExists = stderrors.New("blockList exists")
)

type Config struct {
	Name          string
	Duration      time.Duration
	BlockTimes    int
	BlockDuration time.Duration //0=ever
	BlockError    error
	Redis         *redis.Redis
	WhiteList     []string
	BlockList     []string
	Pub           func(string, string) error
	CustomHandler func(int) error
}

func NewWithConfig(conf *config.Config, c *Config) *RateLimiter {
	c.Name = conf.GetString("application.name") + "-" + c.Name
	return New(c)
}

func New(c *Config) *RateLimiter {
	if c == nil {
		panic("config必须设置")
	}
	if c.Name == "" {
		panic("Name必须设置")
	}
	if c.Duration == 0 {
		panic("Duration必须设置")
	}
	if c.BlockDuration < 0 {
		panic("BlockDuration不能小于0")
	}

	if c.BlockError == nil {
		c.BlockError = ErrorBlock
	}

	rl := RateLimiter{
		Config: c,
	}
	rl.whiteListKey = rl.Name + "-white"
	rl.blockListKey = rl.Name + "-block"
	for _, val := range c.WhiteList {
		rl.whiteList = append(rl.whiteList, val)
	}
	for _, val := range c.BlockList {
		rl.blockList = append(rl.blockList, val)
	}
	whiteList, err := rl.Redis.SMembers(context.Background(), rl.whiteListKey).Result()
	if err == nil {
		for _, val := range whiteList {
			if !funk.ContainsString(rl.whiteList, val) {
				rl.whiteList = append(rl.whiteList, val)
			}
		}
	}
	blockList, err := rl.Redis.SMembers(context.Background(), rl.blockListKey).Result()
	if err == nil {
		for _, val := range blockList {
			if !funk.ContainsString(rl.blockList, val) {
				rl.blockList = append(rl.blockList, val)
			}
		}
	}
	return &rl
}

type RateLimiter struct {
	*Config
	whiteList    []string
	blockList    []string
	whiteListKey string
	blockListKey string
}

func (rl *RateLimiter) Check(id string) (int, error) {
	if funk.Contains(rl.whiteList, id) {
		return 0, nil
	}
	if funk.Contains(rl.blockList, id) {
		return 0, rl.BlockError
	}

	ctx := context.Background()
	times, err := rl.Redis.FrequencyLimit(ctx, rl.Name+":"+id, rl.BlockTimes, rl.Duration)
	if err != nil {
		if err.Error() == "reach limit" {
			return 0, rl.BlockError
		}
		return 0, err
	}
	if rl.BlockTimes > 0 && times >= rl.BlockTimes {
		if rl.BlockDuration == 0 {
			rl.AddBlockList(id, true)
		} else {
			rl.Redis.Expire(ctx, rl.Name+":"+id, rl.BlockDuration)
		}
		return times, rl.BlockError
	}
	if rl.CustomHandler != nil {
		return times, rl.CustomHandler(times)
	}
	return times, nil
}

func (rl *RateLimiter) CheckReset(id string) error {
	_, err := rl.Redis.Del(context.Background(), rl.Name+":"+id).Result()
	return err
}

func (rl *RateLimiter) Sub(message string) error {
	str := strings.Split(message, "-")
	if len(str) != 2 {
		return nil
	}
	switch str[0] {
	case "rw":
		return rl.RemoveWhiteList(str[1], false)
	case "rb":
		return rl.RemoveBlockList(str[1], false)
	case "aw":
		return rl.AddWhiteList(str[1], false)
	case "ab":
		return rl.AddBlockList(str[1], false)
	default:
		return nil
	}
}

func (rl *RateLimiter) RemoveWhiteList(id string, pub bool) error {
	_, err := rl.Redis.SRem(context.Background(), rl.whiteListKey, id).Result()
	if err != nil {
		return err
	}
	idx := funk.IndexOfString(rl.whiteList, id)
	if idx != -1 {
		rl.whiteList = append(rl.whiteList[:idx], rl.whiteList[idx+1:]...)
	}
	if pub && rl.Pub != nil {
		rl.Pub(rl.Name, "rw-"+id)
	}
	return nil
}

func (rl *RateLimiter) RemoveBlockList(id string, pub bool) error {
	_, err := rl.Redis.SRem(context.Background(), rl.blockListKey, id).Result()
	if err != nil {
		return err
	}
	idx := funk.IndexOfString(rl.blockList, id)
	if idx != -1 {
		rl.blockList = append(rl.blockList[:idx], rl.blockList[idx+1:]...)
	}
	if pub && rl.Pub != nil {
		rl.Pub(rl.Name, "rb-"+id)
	}
	return rl.CheckReset(id)
}

func (rl *RateLimiter) AddWhiteList(id string, pub bool) error {
	rl.whiteList = append(rl.whiteList, id)
	_, err := rl.Redis.SAdd(context.Background(), rl.whiteListKey, id).Result()
	if err != nil {
		return err
	}
	idx := funk.IndexOfString(rl.whiteList, id)
	if idx != -1 {
		return ErrorWhiteListExists
	}
	if pub && rl.Pub != nil {
		rl.Pub(rl.Name, "aw-"+id)
	}
	return nil
}

func (rl *RateLimiter) AddBlockList(id string, pub bool) error {
	rl.blockList = append(rl.blockList, id)
	_, err := rl.Redis.SAdd(context.Background(), rl.blockListKey, id).Result()
	if err != nil {
		return err
	}
	idx := funk.IndexOfString(rl.blockList, id)
	if idx != -1 {
		return ErrorBlockListExists
	}
	if pub && rl.Pub != nil {
		rl.Pub(rl.Name, "ab-"+id)
	}
	return nil
}

func (rl *RateLimiter) GetWhiteList(id interface{}) ([]string, error) {
	if id != nil {
		has := funk.ContainsString(rl.whiteList, id.(string))
		if has {
			return []string{id.(string)}, nil
		} else {
			return nil, nil
		}
	}
	return rl.whiteList, nil
}

func (rl *RateLimiter) GetBlockList(id interface{}) ([]string, error) {
	if id != nil {
		has := funk.ContainsString(rl.blockList, id.(string))
		if has {
			return []string{id.(string)}, nil
		} else {
			return nil, nil
		}
	}
	return rl.blockList, nil
}
