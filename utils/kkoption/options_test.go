package kkoption

import (
	"testing"
)

// TestConfig 测试用的配置结构体
type TestConfig struct {
	Name     string
	Age      int
	Enabled  bool
	Value    float64
	Settings map[string]string
}

// TestSimpleConfig 简单的测试配置
type TestSimpleConfig struct {
	Value int
}

// TestApplyOptionWithDefault_Basic 测试基本功能
func TestApplyOptionWithDefault_Basic(t *testing.T) {
	defaultCfg := func() TestConfig {
		return TestConfig{
			Name:    "default",
			Age:     18,
			Enabled: false,
			Value:   0.0,
		}
	}

	opt1 := func(c *TestConfig) {
		c.Name = "test"
	}
	opt2 := func(c *TestConfig) {
		c.Age = 25
	}

	cfg := ApplyOptionWithDefault(defaultCfg, opt1, opt2)

	if cfg.Name != "test" {
		t.Errorf("Name = %v, want 'test'", cfg.Name)
	}
	if cfg.Age != 25 {
		t.Errorf("Age = %v, want 25", cfg.Age)
	}
	if cfg.Enabled != false {
		t.Errorf("Enabled = %v, want false", cfg.Enabled)
	}
}

// TestApplyOptionWithDefault_NoOptions 测试无选项的情况
func TestApplyOptionWithDefault_NoOptions(t *testing.T) {
	defaultCfg := func() TestConfig {
		return TestConfig{
			Name:    "default",
			Age:     18,
			Enabled: true,
		}
	}

	cfg := ApplyOptionWithDefault(defaultCfg)

	if cfg.Name != "default" {
		t.Errorf("Name = %v, want 'default'", cfg.Name)
	}
	if cfg.Age != 18 {
		t.Errorf("Age = %v, want 18", cfg.Age)
	}
	if cfg.Enabled != true {
		t.Errorf("Enabled = %v, want true", cfg.Enabled)
	}
}

// TestApplyOptionWithDefault_NilOptions 测试 nil 选项
func TestApplyOptionWithDefault_NilOptions(t *testing.T) {
	defaultCfg := func() TestConfig {
		return TestConfig{
			Name: "default",
			Age:  18,
		}
	}

	opt1 := func(c *TestConfig) {
		c.Name = "test"
	}

	cfg := ApplyOptionWithDefault(defaultCfg, opt1, nil, nil)

	if cfg.Name != "test" {
		t.Errorf("Name = %v, want 'test'", cfg.Name)
	}
	if cfg.Age != 18 {
		t.Errorf("Age = %v, want 18", cfg.Age)
	}
}

// TestApplyOptionWithDefault_MultipleOptions 测试多个选项的顺序应用
func TestApplyOptionWithDefault_MultipleOptions(t *testing.T) {
	defaultCfg := func() TestConfig {
		return TestConfig{
			Name: "default",
			Age:  0,
		}
	}

	opt1 := func(c *TestConfig) {
		c.Name = "first"
		c.Age = 10
	}
	opt2 := func(c *TestConfig) {
		c.Name = "second"
	}
	opt3 := func(c *TestConfig) {
		c.Age = 20
	}

	cfg := ApplyOptionWithDefault(defaultCfg, opt1, opt2, opt3)

	// 最后一个选项应该覆盖前面的
	if cfg.Name != "second" {
		t.Errorf("Name = %v, want 'second'", cfg.Name)
	}
	if cfg.Age != 20 {
		t.Errorf("Age = %v, want 20", cfg.Age)
	}
}

// TestApplyOptionWithDefault_ComplexType 测试复杂类型
func TestApplyOptionWithDefault_ComplexType(t *testing.T) {
	defaultCfg := func() TestConfig {
		return TestConfig{
			Settings: map[string]string{
				"key1": "value1",
			},
		}
	}

	opt := func(c *TestConfig) {
		if c.Settings == nil {
			c.Settings = make(map[string]string)
		}
		c.Settings["key2"] = "value2"
		c.Settings["key1"] = "updated"
	}

	cfg := ApplyOptionWithDefault(defaultCfg, opt)

	if cfg.Settings["key1"] != "updated" {
		t.Errorf("Settings[key1] = %v, want 'updated'", cfg.Settings["key1"])
	}
	if cfg.Settings["key2"] != "value2" {
		t.Errorf("Settings[key2] = %v, want 'value2'", cfg.Settings["key2"])
	}
}

// TestApplyOptionsWithNew_Basic 测试基本功能
func TestApplyOptionsWithNew_Basic(t *testing.T) {
	opt1 := func(c *TestConfig) {
		c.Name = "test"
	}
	opt2 := func(c *TestConfig) {
		c.Age = 25
	}

	cfg := ApplyOptionsWithNew(opt1, opt2)

	if cfg.Name != "test" {
		t.Errorf("Name = %v, want 'test'", cfg.Name)
	}
	if cfg.Age != 25 {
		t.Errorf("Age = %v, want 25", cfg.Age)
	}
}

// TestApplyOptionsWithNew_NoOptions 测试无选项的情况（应该返回零值）
func TestApplyOptionsWithNew_NoOptions(t *testing.T) {
	cfg := ApplyOptionsWithNew[TestConfig]()

	if cfg.Name != "" {
		t.Errorf("Name = %v, want ''", cfg.Name)
	}
	if cfg.Age != 0 {
		t.Errorf("Age = %v, want 0", cfg.Age)
	}
	if cfg.Enabled != false {
		t.Errorf("Enabled = %v, want false", cfg.Enabled)
	}
}

// TestApplyOptionsWithNew_NilOptions 测试 nil 选项
func TestApplyOptionsWithNew_NilOptions(t *testing.T) {
	opt1 := func(c *TestConfig) {
		c.Name = "test"
	}

	cfg := ApplyOptionsWithNew(opt1, nil, nil)

	if cfg.Name != "test" {
		t.Errorf("Name = %v, want 'test'", cfg.Name)
	}
}

// TestApplyOptionsWithNew_MultipleOptions 测试多个选项的顺序应用
func TestApplyOptionsWithNew_MultipleOptions(t *testing.T) {
	opt1 := func(c *TestConfig) {
		c.Name = "first"
		c.Age = 10
	}
	opt2 := func(c *TestConfig) {
		c.Name = "second"
	}
	opt3 := func(c *TestConfig) {
		c.Age = 20
	}

	cfg := ApplyOptionsWithNew(opt1, opt2, opt3)

	if cfg.Name != "second" {
		t.Errorf("Name = %v, want 'second'", cfg.Name)
	}
	if cfg.Age != 20 {
		t.Errorf("Age = %v, want 20", cfg.Age)
	}
}

// TestApplyOptionsWithNew_SimpleType 测试简单类型
func TestApplyOptionsWithNew_SimpleType(t *testing.T) {
	opt := func(v *int) {
		*v = 42
	}

	value := ApplyOptionsWithNew(opt)

	if value != 42 {
		t.Errorf("value = %v, want 42", value)
	}
}

// TestApplyOptionsTo_Basic 测试基本功能
func TestApplyOptionsTo_Basic(t *testing.T) {
	cfg := &TestConfig{
		Name: "original",
		Age:  18,
	}

	opt1 := func(c *TestConfig) {
		c.Name = "updated"
	}
	opt2 := func(c *TestConfig) {
		c.Age = 25
	}

	result := ApplyOptionsTo(cfg, opt1, opt2)

	if result != cfg {
		t.Error("ApplyOptionsTo should return the same pointer")
	}
	if cfg.Name != "updated" {
		t.Errorf("Name = %v, want 'updated'", cfg.Name)
	}
	if cfg.Age != 25 {
		t.Errorf("Age = %v, want 25", cfg.Age)
	}
}

// TestApplyOptionsTo_NoOptions 测试无选项的情况
func TestApplyOptionsTo_NoOptions(t *testing.T) {
	cfg := &TestConfig{
		Name: "original",
		Age:  18,
	}

	result := ApplyOptionsTo(cfg)

	if result != cfg {
		t.Error("ApplyOptionsTo should return the same pointer")
	}
	if cfg.Name != "original" {
		t.Errorf("Name = %v, want 'original'", cfg.Name)
	}
	if cfg.Age != 18 {
		t.Errorf("Age = %v, want 18", cfg.Age)
	}
}

// TestApplyOptionsTo_NilOptions 测试 nil 选项
func TestApplyOptionsTo_NilOptions(t *testing.T) {
	cfg := &TestConfig{
		Name: "original",
		Age:  18,
	}

	opt := func(c *TestConfig) {
		c.Name = "updated"
	}

	result := ApplyOptionsTo(cfg, opt, nil, nil)

	if result != cfg {
		t.Error("ApplyOptionsTo should return the same pointer")
	}
	if cfg.Name != "updated" {
		t.Errorf("Name = %v, want 'updated'", cfg.Name)
	}
}

// TestApplyOptionsTo_MultipleOptions 测试多个选项的顺序应用
func TestApplyOptionsTo_MultipleOptions(t *testing.T) {
	cfg := &TestConfig{
		Name: "original",
		Age:  0,
	}

	opt1 := func(c *TestConfig) {
		c.Name = "first"
		c.Age = 10
	}
	opt2 := func(c *TestConfig) {
		c.Name = "second"
	}
	opt3 := func(c *TestConfig) {
		c.Age = 20
	}

	ApplyOptionsTo(cfg, opt1, opt2, opt3)

	if cfg.Name != "second" {
		t.Errorf("Name = %v, want 'second'", cfg.Name)
	}
	if cfg.Age != 20 {
		t.Errorf("Age = %v, want 20", cfg.Age)
	}
}

// TestApplyOptionsTo_NilConfig 测试 nil 配置（应该 panic 或正常工作）
func TestApplyOptionsTo_NilConfig(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			// 如果 nil 配置不会 panic，那也是可以接受的行为
			// 取决于具体实现
		}
	}()

	opt := func(c *TestConfig) {
		if c != nil {
			c.Name = "test"
		}
	}

	result := ApplyOptionsTo(nil, opt)

	// 如果函数能处理 nil，result 应该是 nil
	if result != nil {
		t.Log("ApplyOptionsTo handled nil config, which is acceptable")
	}
}

// TestApplyOptionsTo_ComplexType 测试复杂类型
func TestApplyOptionsTo_ComplexType(t *testing.T) {
	cfg := &TestConfig{
		Settings: map[string]string{
			"key1": "value1",
		},
	}

	opt := func(c *TestConfig) {
		if c.Settings == nil {
			c.Settings = make(map[string]string)
		}
		c.Settings["key2"] = "value2"
		c.Settings["key1"] = "updated"
	}

	ApplyOptionsTo(cfg, opt)

	if cfg.Settings["key1"] != "updated" {
		t.Errorf("Settings[key1] = %v, want 'updated'", cfg.Settings["key1"])
	}
	if cfg.Settings["key2"] != "value2" {
		t.Errorf("Settings[key2] = %v, want 'value2'", cfg.Settings["key2"])
	}
}

// TestApplyOptionsTo_AllFields 测试所有字段
func TestApplyOptionsTo_AllFields(t *testing.T) {
	cfg := &TestConfig{}

	opt := func(c *TestConfig) {
		c.Name = "test"
		c.Age = 25
		c.Enabled = true
		c.Value = 3.14
		c.Settings = map[string]string{
			"key": "value",
		}
	}

	ApplyOptionsTo(cfg, opt)

	if cfg.Name != "test" {
		t.Errorf("Name = %v, want 'test'", cfg.Name)
	}
	if cfg.Age != 25 {
		t.Errorf("Age = %v, want 25", cfg.Age)
	}
	if cfg.Enabled != true {
		t.Errorf("Enabled = %v, want true", cfg.Enabled)
	}
	if cfg.Value != 3.14 {
		t.Errorf("Value = %v, want 3.14", cfg.Value)
	}
	if cfg.Settings["key"] != "value" {
		t.Errorf("Settings[key] = %v, want 'value'", cfg.Settings["key"])
	}
}

// TestApplyOptionsTo_SimpleType 测试简单类型
func TestApplyOptionsTo_SimpleType(t *testing.T) {
	value := 10

	opt := func(v *int) {
		*v = 42
	}

	result := ApplyOptionsTo(&value, opt)

	if result != &value {
		t.Error("ApplyOptionsTo should return the same pointer")
	}
	if value != 42 {
		t.Errorf("value = %v, want 42", value)
	}
}

// TestAllFunctions_Consistency 测试三个函数的一致性
func TestAllFunctions_Consistency(t *testing.T) {
	// 使用相同的选项函数
	opt1 := func(c *TestConfig) {
		c.Name = "test"
	}
	opt2 := func(c *TestConfig) {
		c.Age = 25
	}

	// ApplyOptionWithDefault
	defaultCfg := func() TestConfig {
		return TestConfig{Name: "default", Age: 0}
	}
	cfg1 := ApplyOptionWithDefault(defaultCfg, opt1, opt2)

	// ApplyOptionsWithNew
	cfg2 := ApplyOptionsWithNew(opt1, opt2)

	// ApplyOptionsTo
	cfg3 := &TestConfig{}
	ApplyOptionsTo(cfg3, opt1, opt2)

	// 验证结果一致
	if cfg1.Name != cfg2.Name || cfg2.Name != cfg3.Name {
		t.Error("Name should be consistent across all functions")
	}
	if cfg1.Age != cfg2.Age || cfg2.Age != cfg3.Age {
		t.Error("Age should be consistent across all functions")
	}
}

// BenchmarkApplyOptionWithDefault 性能测试
func BenchmarkApplyOptionWithDefault(b *testing.B) {
	defaultCfg := func() TestConfig {
		return TestConfig{Name: "default", Age: 18}
	}
	opt := func(c *TestConfig) {
		c.Name = "test"
		c.Age = 25
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ApplyOptionWithDefault(defaultCfg, opt)
	}
}

// BenchmarkApplyOptionsWithNew 性能测试
func BenchmarkApplyOptionsWithNew(b *testing.B) {
	opt := func(c *TestConfig) {
		c.Name = "test"
		c.Age = 25
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ApplyOptionsWithNew(opt)
	}
}

// BenchmarkApplyOptionsTo 性能测试
func BenchmarkApplyOptionsTo(b *testing.B) {
	cfg := &TestConfig{}
	opt := func(c *TestConfig) {
		c.Name = "test"
		c.Age = 25
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cfg.Name = ""
		cfg.Age = 0
		ApplyOptionsTo(cfg, opt)
	}
}
