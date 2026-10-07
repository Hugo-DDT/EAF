package io.eaf.skill.api;

// 返回依赖版本和 Schema 摘要，避免 Skill 读取权限顺带泄露 Tool 的完整声明。
public record SkillToolDependency(String name, String version, String inputSchemaHash, String outputSchemaHash) { }
