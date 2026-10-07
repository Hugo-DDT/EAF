package io.eaf.skill.api;

// Skill 引用明确版本的 Tool；权限仍由 Workspace、Policy 和 Execution 独立决定。
public record SkillToolReference(String name, String version) { }
