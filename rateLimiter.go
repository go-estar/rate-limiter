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
	ErrorWarning         = stderrors.New("too many requests")
	ErrorWhiteListExists = stderrors.New("whiteList exists")
	ErrorBlackListExists = stderrors.New("blackList exists")
)

type Config struct {
	Name          string
	Duration      time.Duration
	WarningTimes  int
	BlockTimes    int
	BlockDuration time.Duration //0=ever
	WarningError  error
	BlockError    error
	Store         *redis.Redis
	WhiteList     []string
	BlackList     []string
	Pub           func(string, string) error
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
	if c.WarningError == nil {
		c.WarningError = ErrorWarning
	}

	rl := RateLimiter{
		Config: c,
	}
	rl.whiteListKey = rl.Name + "-white"
	rl.blackListKey = rl.Name + "-black"
	for _, val := range c.WhiteList {
		rl.whiteList = append(rl.whiteList, val)
	}
	for _, val := range c.BlackList {
		rl.blackList = append(rl.blackList, val)
	}
	whiteList, err := rl.Store.SMembers(context.Background(), rl.whiteListKey).Result()
	if err == nil {
		for _, val := range whiteList {
			if !funk.ContainsString(rl.whiteList, val) {
				rl.whiteList = append(rl.whiteList, val)
			}
		}
	}
	blackList, err := rl.Store.SMembers(context.Background(), rl.blackListKey).Result()
	if err == nil {
		for _, val := range blackList {
			if !funk.ContainsString(rl.blackList, val) {
				rl.blackList = append(rl.blackList, val)
			}
		}
	}
	return &rl
}

type RateLimiter struct {
	*Config
	whiteList    []string
	blackList    []string
	whiteListKey string
	blackListKey string
}

func (rl *RateLimiter) Check(id string) (int, error) {
	if funk.Contains(rl.whiteList, id) {
		return 0, nil
	}

	if funk.Contains(rl.blackList, id) {
		return 0, rl.BlockError
	}

	times, err := rl.Store.FrequencyLimit(context.Background(), rl.Name+":"+id, 0, rl.Duration)
	if err != nil {
		return 0, err
	}

	if rl.BlockTimes > 0 && times >= rl.BlockTimes {
		if rl.BlockDuration == 0 {
			rl.AddBlackList(id, true)
		} else {
			rl.Store.Expire(context.Background(), rl.Name+":"+id, rl.BlockDuration)
		}
		return times, rl.BlockError
	} else if rl.WarningTimes > 0 && times >= rl.WarningTimes {
		return times, rl.WarningError
	}

	return times, nil
}

func (rl *RateLimiter) CheckReset(id string) error {
	_, err := rl.Store.Del(context.Background(), rl.Name+":"+id).Result()
	return err
}

func (rl *RateLimiter) Sub(message string) error {
	str := strings.Split(message, "-")
	if len(str) != 2 {
		return nil
	}
	switch str[0] {
	case "removeWhiteList":
		return rl.RemoveWhiteList(str[1], false)
	case "removeBlackList":
		return rl.RemoveBlackList(str[1], false)
	case "addWhiteList":
		return rl.AddWhiteList(str[1], false)
	case "addBlackList":
		return rl.AddBlackList(str[1], false)
	default:
		return nil
	}
}

func (rl *RateLimiter) RemoveWhiteList(id string, pub bool) error {
	_, err := rl.Store.SRem(context.Background(), rl.whiteListKey, id).Result()
	if err != nil {
		return err
	}
	idx := funk.IndexOfString(rl.whiteList, id)
	if idx != -1 {
		rl.whiteList = append(rl.whiteList[:idx], rl.whiteList[idx+1:]...)
	}
	if pub && rl.Pub != nil {
		rl.Pub(rl.Name, "removeWhiteList-"+id)
	}
	return nil
}

func (rl *RateLimiter) RemoveBlackList(id string, pub bool) error {
	_, err := rl.Store.SRem(context.Background(), rl.blackListKey, id).Result()
	if err != nil {
		return err
	}
	idx := funk.IndexOfString(rl.blackList, id)
	if idx != -1 {
		rl.blackList = append(rl.blackList[:idx], rl.blackList[idx+1:]...)
	}
	if pub && rl.Pub != nil {
		rl.Pub(rl.Name, "removeBlackList-"+id)
	}
	return rl.CheckReset(id)
}

func (rl *RateLimiter) AddWhiteList(id string, pub bool) error {
	_, err := rl.Store.SAdd(context.Background(), rl.whiteListKey, id).Result()
	if err != nil {
		return err
	}

	idx := funk.IndexOfString(rl.whiteList, id)
	if idx != -1 {
		return ErrorWhiteListExists
	}
	rl.whiteList = append(rl.whiteList, id)
	if pub && rl.Pub != nil {
		rl.Pub(rl.Name, "addWhiteList-"+id)
	}
	return nil
}

func (rl *RateLimiter) AddBlackList(id string, pub bool) error {
	_, err := rl.Store.SAdd(context.Background(), rl.blackListKey, id).Result()
	if err != nil {
		return err
	}
	idx := funk.IndexOfString(rl.blackList, id)
	if idx != -1 {
		return ErrorBlackListExists
	}
	rl.blackList = append(rl.blackList, id)
	if pub && rl.Pub != nil {
		rl.Pub(rl.Name, "addBlackList-"+id)
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

func (rl *RateLimiter) GetBlackList(id interface{}) ([]string, error) {
	if id != nil {
		has := funk.ContainsString(rl.blackList, id.(string))
		if has {
			return []string{id.(string)}, nil
		} else {
			return nil, nil
		}
	}
	return rl.blackList, nil
}
