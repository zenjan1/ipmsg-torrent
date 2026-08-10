//! Download Dependency Graph Validator (Phase 113)
//!
//! Validates the integrity of the download task dependency graph,
//! detects issues like orphaned dependencies, provides topological
//! ordering, and graph analysis statistics.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

/// Configuration for the dependency graph validator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DependencyGraphConfig {
    /// Enable automatic validation on dependency changes.
    pub auto_validate: bool,
    /// Maximum dependency depth before warning (prevents excessively deep chains).
    pub max_depth_warning_threshold: usize,
    /// Whether to include completed tasks in graph analysis.
    pub include_completed_in_analysis: bool,
}

impl Default for DependencyGraphConfig {
    fn default() -> Self {
        Self {
            auto_validate: true,
            max_depth_warning_threshold: 10,
            include_completed_in_analysis: true,
        }
    }
}

/// Severity of a dependency graph issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IssueSeverity {
    /// Informational note.
    Info,
    /// Potential problem that may affect behavior.
    Warning,
    /// Definite problem that will cause incorrect behavior.
    Error,
}

/// Category of a dependency graph issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IssueCategory {
    /// Dependency references a non-existent task.
    OrphanedDependency,
    /// Circular dependency chain detected.
    CircularDependency,
    /// Task depends on itself.
    SelfDependency,
    /// Dependency chain exceeds maximum depth.
    ExcessiveDepth,
    /// Dependency on a failed/errored task.
    BlockedByFailure,
    /// Redundant transitive dependency (A→B→C and A→C).
    RedundantDependency,
}

/// A single issue found in the dependency graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DependencyIssue {
    /// The task ID where the issue was found.
    pub task_id: String,
    /// Category of the issue.
    pub category: IssueCategory,
    /// Severity of the issue.
    pub severity: IssueSeverity,
    /// Human-readable description of the issue.
    pub message: String,
    /// Related task IDs (e.g., the orphaned dep ID, cycle members).
    pub related_ids: Vec<String>,
}

/// Result of validating the dependency graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Whether the graph is valid (no errors).
    pub is_valid: bool,
    /// All issues found (warnings + errors).
    pub issues: Vec<DependencyIssue>,
    /// Number of errors (severity = Error).
    pub error_count: usize,
    /// Number of warnings (severity = Warning).
    pub warning_count: usize,
    /// Number of info items (severity = Info).
    pub info_count: usize,
}

impl ValidationResult {
    /// Get only the error-level issues.
    pub fn errors(&self) -> Vec<&DependencyIssue> {
        self.issues
            .iter()
            .filter(|i| i.severity == IssueSeverity::Error)
            .collect()
    }

    /// Get only the warning-level issues.
    pub fn warnings(&self) -> Vec<&DependencyIssue> {
        self.issues
            .iter()
            .filter(|i| i.severity == IssueSeverity::Warning)
            .collect()
    }

    /// Format a human-readable summary of the validation result.
    pub fn format_summary(&self) -> String {
        if self.is_valid && self.issues.is_empty() {
            return "✅ Dependency graph is valid. No issues found.".to_string();
        }

        let mut parts = Vec::new();
        if self.error_count > 0 {
            parts.push(format!("❌ {} error(s)", self.error_count));
        }
        if self.warning_count > 0 {
            parts.push(format!("⚠️ {} warning(s)", self.warning_count));
        }
        if self.info_count > 0 {
            parts.push(format!("ℹ️ {} info", self.info_count));
        }

        let mut result = format!("Dependency graph validation: {}", parts.join(", "));

        for issue in &self.issues {
            let icon = match issue.severity {
                IssueSeverity::Info => "ℹ️",
                IssueSeverity::Warning => "⚠️",
                IssueSeverity::Error => "❌",
            };
            result.push_str(&format!(
                "\n  {} [{}] {}",
                icon, issue.task_id, issue.message
            ));
            if !issue.related_ids.is_empty() {
                result.push_str(&format!(" (related: {})", issue.related_ids.join(", ")));
            }
        }

        result
    }
}

/// Statistics about the dependency graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphStats {
    /// Total number of tasks in the graph.
    pub total_tasks: usize,
    /// Number of tasks with at least one dependency.
    pub tasks_with_deps: usize,
    /// Number of tasks with no dependencies (root tasks).
    pub root_tasks: usize,
    /// Number of tasks that are depended upon by others.
    pub depended_upon_count: usize,
    /// Total number of dependency edges.
    pub total_edges: usize,
    /// Maximum depth of any dependency chain.
    pub max_chain_depth: usize,
    /// Number of independent subgraphs (connected components).
    pub connected_components: usize,
    /// Average number of dependencies per task (that have deps).
    pub avg_deps_per_task: f64,
    /// Tasks that form the longest chain (from root to leaf).
    pub longest_chain: Vec<String>,
}

impl GraphStats {
    /// Format a human-readable summary.
    pub fn format_summary(&self) -> String {
        let mut result = String::new();
        result.push_str(&format!("📊 Dependency Graph Statistics:\n"));
        result.push_str(&format!("  Total tasks: {}\n", self.total_tasks));
        result.push_str(&format!(
            "  Tasks with dependencies: {}\n",
            self.tasks_with_deps
        ));
        result.push_str(&format!("  Root tasks (no deps): {}\n", self.root_tasks));
        result.push_str(&format!(
            "  Tasks depended upon: {}\n",
            self.depended_upon_count
        ));
        result.push_str(&format!("  Total dependency edges: {}\n", self.total_edges));
        result.push_str(&format!("  Max chain depth: {}\n", self.max_chain_depth));
        result.push_str(&format!(
            "  Connected components: {}\n",
            self.connected_components
        ));
        result.push_str(&format!(
            "  Avg deps per task (with deps): {:.2}\n",
            self.avg_deps_per_task
        ));
        if !self.longest_chain.is_empty() {
            result.push_str(&format!(
                "  Longest chain: {}\n",
                self.longest_chain.join(" → ")
            ));
        }
        result
    }
}

/// Topological ordering result.
#[derive(Debug, Clone)]
pub struct TopologicalOrder {
    /// Task IDs in topological order (dependencies come before dependents).
    pub order: Vec<String>,
    /// Whether the ordering is complete (all tasks included).
    /// False if there are cycles preventing full ordering.
    pub is_complete: bool,
    /// Tasks that could not be ordered due to cycles.
    pub unorderable: Vec<String>,
}

impl TopologicalOrder {
    /// Format the ordering as a human-readable list.
    pub fn format_order(&self) -> String {
        if self.order.is_empty() {
            return "No tasks to order.".to_string();
        }
        let mut result = String::new();
        for (i, task_id) in self.order.iter().enumerate() {
            result.push_str(&format!("  {}. {}\n", i + 1, task_id));
        }
        if !self.is_complete {
            result.push_str(&format!(
                "\n⚠️ {} task(s) could not be ordered due to cycles:\n",
                self.unorderable.len()
            ));
            for task_id in &self.unorderable {
                result.push_str(&format!("  - {}\n", task_id));
            }
        }
        result
    }
}

/// Input data for a single task needed by the validator.
#[derive(Debug, Clone)]
pub struct TaskDepData {
    /// Task ID.
    pub id: String,
    /// Task IDs this task depends on.
    pub depends_on: Vec<String>,
    /// Whether the task is complete.
    pub is_complete: bool,
    /// Whether the task is in error state.
    pub is_error: bool,
}

/// The dependency graph validator.
#[derive(Debug, Clone)]
pub struct DependencyGraphValidator {
    config: DependencyGraphConfig,
}

impl DependencyGraphValidator {
    /// Create a new validator with default configuration.
    pub fn new() -> Self {
        Self {
            config: DependencyGraphConfig::default(),
        }
    }

    /// Create a new validator with the given configuration.
    pub fn with_config(config: DependencyGraphConfig) -> Self {
        Self { config }
    }

    /// Get the current configuration.
    pub fn config(&self) -> &DependencyGraphConfig {
        &self.config
    }

    /// Update the configuration.
    pub fn set_config(&mut self, config: DependencyGraphConfig) {
        self.config = config;
    }

    /// Validate the dependency graph for issues.
    pub fn validate(&self, tasks: &[TaskDepData]) -> ValidationResult {
        let mut issues = Vec::new();
        let task_ids: HashSet<&str> = tasks.iter().map(|t| t.id.as_str()).collect();
        let task_map: HashMap<&str, &TaskDepData> =
            tasks.iter().map(|t| (t.id.as_str(), t)).collect();

        for task in tasks {
            // Check self-dependency
            if task.depends_on.contains(&task.id) {
                issues.push(DependencyIssue {
                    task_id: task.id.clone(),
                    category: IssueCategory::SelfDependency,
                    severity: IssueSeverity::Error,
                    message: "Task depends on itself".to_string(),
                    related_ids: vec![task.id.clone()],
                });
            }

            for dep_id in &task.depends_on {
                // Check orphaned dependencies
                if !task_ids.contains(dep_id.as_str()) {
                    issues.push(DependencyIssue {
                        task_id: task.id.clone(),
                        category: IssueCategory::OrphanedDependency,
                        severity: IssueSeverity::Error,
                        message: format!("Dependency '{}' does not exist", dep_id),
                        related_ids: vec![dep_id.clone()],
                    });
                    continue;
                }

                // Check dependency on errored task
                if let Some(dep_task) = task_map.get(dep_id.as_str()) {
                    if dep_task.is_error {
                        issues.push(DependencyIssue {
                            task_id: task.id.clone(),
                            category: IssueCategory::BlockedByFailure,
                            severity: IssueSeverity::Warning,
                            message: format!(
                                "Dependency '{}' is in error state, task will never start",
                                dep_id
                            ),
                            related_ids: vec![dep_id.clone()],
                        });
                    }
                }
            }

            // Check redundant transitive dependencies
            self.check_redundant_deps(task, &task_map, &mut issues);
        }

        // Check circular dependencies
        self.check_cycles(tasks, &mut issues);

        // Check excessive depth
        self.check_depth(tasks, &mut issues);

        let error_count = issues
            .iter()
            .filter(|i| i.severity == IssueSeverity::Error)
            .count();
        let warning_count = issues
            .iter()
            .filter(|i| i.severity == IssueSeverity::Warning)
            .count();
        let info_count = issues
            .iter()
            .filter(|i| i.severity == IssueSeverity::Info)
            .count();

        ValidationResult {
            is_valid: error_count == 0,
            issues,
            error_count,
            warning_count,
            info_count,
        }
    }

    /// Check for circular dependencies using DFS.
    fn check_cycles(&self, tasks: &[TaskDepData], issues: &mut Vec<DependencyIssue>) {
        let task_map: HashMap<&str, &TaskDepData> =
            tasks.iter().map(|t| (t.id.as_str(), t)).collect();

        // States: 0 = unvisited, 1 = in progress, 2 = done
        let mut state: HashMap<&str, u8> = HashMap::new();
        let mut path: Vec<&str> = Vec::new();
        let mut reported_cycles: HashSet<Vec<String>> = HashSet::new();

        for task in tasks {
            if state.get(task.id.as_str()).copied().unwrap_or(0) == 0 {
                self.dfs_cycle(
                    &task.id,
                    &task_map,
                    &mut state,
                    &mut path,
                    issues,
                    &mut reported_cycles,
                );
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn dfs_cycle<'a>(
        &self,
        node_id: &'a str,
        task_map: &HashMap<&'a str, &'a TaskDepData>,
        state: &mut HashMap<&'a str, u8>,
        path: &mut Vec<&'a str>,
        issues: &mut Vec<DependencyIssue>,
        reported_cycles: &mut HashSet<Vec<String>>,
    ) {
        state.insert(node_id, 1);
        path.push(node_id);

        if let Some(task) = task_map.get(node_id) {
            for dep_id in &task.depends_on {
                let dep_str = dep_id.as_str();
                // Skip self-edges (already caught by self-dependency check)
                if dep_str == node_id {
                    continue;
                }
                match state.get(dep_str).copied().unwrap_or(0) {
                    1 => {
                        // Found a cycle - extract it
                        if let Some(cycle_start) = path.iter().position(|&p| p == dep_str) {
                            let mut cycle: Vec<String> =
                                path[cycle_start..].iter().map(|s| s.to_string()).collect();
                            cycle.sort();
                            if reported_cycles.insert(cycle.clone()) {
                                let cycle_members: Vec<String> =
                                    path[cycle_start..].iter().map(|s| s.to_string()).collect();
                                issues.push(DependencyIssue {
                                    task_id: node_id.to_string(),
                                    category: IssueCategory::CircularDependency,
                                    severity: IssueSeverity::Error,
                                    message: format!(
                                        "Circular dependency detected: {}",
                                        cycle_members.join(" → ")
                                    ),
                                    related_ids: cycle_members,
                                });
                            }
                        }
                    }
                    0 => {
                        self.dfs_cycle(dep_str, task_map, state, path, issues, reported_cycles);
                    }
                    _ => {} // Already fully processed
                }
            }
        }

        path.pop();
        state.insert(node_id, 2);
    }

    /// Check for redundant transitive dependencies.
    /// If A depends on B and C, and B also depends on C, then A→C is redundant.
    fn check_redundant_deps(
        &self,
        task: &TaskDepData,
        task_map: &HashMap<&str, &TaskDepData>,
        issues: &mut Vec<DependencyIssue>,
    ) {
        if task.depends_on.len() < 2 {
            return;
        }

        for dep_id in &task.depends_on {
            // Get transitive deps of this dependency
            let mut transitive_deps = HashSet::new();
            let mut queue = VecDeque::new();
            queue.push_back(dep_id.as_str());
            let mut visited = HashSet::new();
            visited.insert(dep_id.as_str());

            while let Some(current) = queue.pop_front() {
                if let Some(dep_task) = task_map.get(current) {
                    for d in &dep_task.depends_on {
                        if visited.insert(d.as_str()) {
                            transitive_deps.insert(d.as_str());
                            queue.push_back(d.as_str());
                        }
                    }
                }
            }

            // Check if any of our other direct deps are in the transitive set
            for other_dep in &task.depends_on {
                if other_dep != dep_id && transitive_deps.contains(other_dep.as_str()) {
                    issues.push(DependencyIssue {
                        task_id: task.id.clone(),
                        category: IssueCategory::RedundantDependency,
                        severity: IssueSeverity::Info,
                        message: format!(
                            "Dependency '{}' is redundant (already transitively required via '{}')",
                            other_dep, dep_id
                        ),
                        related_ids: vec![other_dep.clone(), dep_id.clone()],
                    });
                }
            }
        }
    }

    /// Check for excessively deep dependency chains.
    fn check_depth(&self, tasks: &[TaskDepData], issues: &mut Vec<DependencyIssue>) {
        let task_map: HashMap<&str, &TaskDepData> =
            tasks.iter().map(|t| (t.id.as_str(), t)).collect();

        let mut memo: HashMap<&str, usize> = HashMap::new();

        for task in tasks {
            let depth = self.compute_depth(&task.id, &task_map, &mut memo, &mut HashSet::new());
            if depth >= self.config.max_depth_warning_threshold {
                issues.push(DependencyIssue {
                    task_id: task.id.clone(),
                    category: IssueCategory::ExcessiveDepth,
                    severity: IssueSeverity::Warning,
                    message: format!(
                        "Dependency chain depth {} exceeds threshold of {}",
                        depth, self.config.max_depth_warning_threshold
                    ),
                    related_ids: Vec::new(),
                });
            }
        }
    }

    /// Compute the depth of a task's dependency chain.
    fn compute_depth<'a>(
        &self,
        task_id: &'a str,
        task_map: &HashMap<&'a str, &'a TaskDepData>,
        memo: &mut HashMap<&'a str, usize>,
        visiting: &mut HashSet<String>,
    ) -> usize {
        if let Some(&depth) = memo.get(task_id) {
            return depth;
        }
        if !visiting.insert(task_id.to_string()) {
            return 0; // Cycle, don't recurse infinitely
        }

        let Some(task) = task_map.get(task_id) else {
            visiting.remove(task_id);
            return 0;
        };

        let max_child_depth = task
            .depends_on
            .iter()
            .map(|dep| self.compute_depth(dep, task_map, memo, visiting))
            .max()
            .unwrap_or(0);

        let depth = if task.depends_on.is_empty() {
            0
        } else {
            max_child_depth + 1
        };

        memo.insert(task_id, depth);
        visiting.remove(task_id);
        depth
    }

    /// Compute topological ordering of tasks using Kahn's algorithm.
    pub fn topological_sort(&self, tasks: &[TaskDepData]) -> TopologicalOrder {
        let task_ids: HashSet<&str> = tasks.iter().map(|t| t.id.as_str()).collect();
        let _task_map: HashMap<&str, &TaskDepData> =
            tasks.iter().map(|t| (t.id.as_str(), t)).collect();

        // Build in-degree map (only counting edges to existing tasks)
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        let mut adjacency: HashMap<&str, Vec<&str>> = HashMap::new();

        for task in tasks {
            in_degree.entry(task.id.as_str()).or_insert(0);
            adjacency.entry(task.id.as_str()).or_default();

            for dep_id in &task.depends_on {
                if task_ids.contains(dep_id.as_str()) {
                    // dep_id → task.id (dep must come before task)
                    adjacency
                        .entry(dep_id.as_str())
                        .or_default()
                        .push(task.id.as_str());
                    *in_degree.entry(task.id.as_str()).or_insert(0) += 1;
                }
            }
        }

        // Start with nodes that have no dependencies
        let mut queue: VecDeque<&str> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(id, _)| *id)
            .collect();

        // Sort for deterministic output
        let mut queue_vec: Vec<&str> = queue.drain(..).collect();
        queue_vec.sort();
        queue.extend(queue_vec);

        let mut order = Vec::new();

        while let Some(node) = queue.pop_front() {
            order.push(node.to_string());

            if let Some(neighbors) = adjacency.get(node) {
                let mut newly_ready: Vec<&str> = Vec::new();
                for &neighbor in neighbors {
                    if let Some(deg) = in_degree.get_mut(neighbor) {
                        *deg -= 1;
                        if *deg == 0 {
                            newly_ready.push(neighbor);
                        }
                    }
                }
                newly_ready.sort();
                queue.extend(newly_ready);
            }
        }

        let is_complete = order.len() == tasks.len();
        let ordered_set: HashSet<&str> = order.iter().map(|s| s.as_str()).collect();
        let unorderable: Vec<String> = tasks
            .iter()
            .filter(|t| !ordered_set.contains(t.id.as_str()))
            .map(|t| t.id.clone())
            .collect();

        TopologicalOrder {
            order,
            is_complete,
            unorderable,
        }
    }

    /// Compute statistics about the dependency graph.
    pub fn compute_stats(&self, tasks: &[TaskDepData]) -> GraphStats {
        let total_tasks = tasks.len();
        let task_ids: HashSet<&str> = tasks.iter().map(|t| t.id.as_str()).collect();

        let tasks_with_deps = tasks.iter().filter(|t| !t.depends_on.is_empty()).count();
        let root_tasks = total_tasks - tasks_with_deps;

        // Count how many tasks are depended upon
        let mut depended_upon: HashSet<&str> = HashSet::new();
        let mut total_edges = 0;
        for task in tasks {
            for dep_id in &task.depends_on {
                if task_ids.contains(dep_id.as_str()) {
                    depended_upon.insert(dep_id.as_str());
                    total_edges += 1;
                }
            }
        }
        let depended_upon_count = depended_upon.len();

        // Average deps per task (only those with deps)
        let avg_deps_per_task = if tasks_with_deps > 0 {
            total_edges as f64 / tasks_with_deps as f64
        } else {
            0.0
        };

        // Build task_map for depth computation
        let task_map: HashMap<&str, &TaskDepData> =
            tasks.iter().map(|t| (t.id.as_str(), t)).collect();

        // Compute max chain depth and longest chain
        let mut memo: HashMap<&str, usize> = HashMap::new();
        let mut max_depth = 0;
        let mut deepest_task_id: Option<&str> = None;

        for task in tasks {
            let depth = self.compute_depth(&task.id, &task_map, &mut memo, &mut HashSet::new());
            if depth > max_depth {
                max_depth = depth;
                deepest_task_id = Some(task.id.as_str());
            }
        }

        // Trace the longest chain
        let longest_chain = deepest_task_id
            .map(|id| self.trace_longest_chain(id, &task_map))
            .unwrap_or_default();

        // Compute connected components using union-find
        let connected_components = self.count_connected_components(tasks, &task_ids);

        GraphStats {
            total_tasks,
            tasks_with_deps,
            root_tasks,
            depended_upon_count,
            total_edges,
            max_chain_depth: max_depth,
            connected_components,
            avg_deps_per_task,
            longest_chain,
        }
    }

    /// Trace the longest dependency chain from a given task back to a root.
    fn trace_longest_chain(
        &self,
        task_id: &str,
        task_map: &HashMap<&str, &TaskDepData>,
    ) -> Vec<String> {
        let mut chain = vec![task_id.to_string()];
        let mut current = task_id;
        let mut visited = HashSet::new();
        visited.insert(current);

        loop {
            let Some(task) = task_map.get(current) else {
                break;
            };
            if task.depends_on.is_empty() {
                break;
            }

            // Find the dependency with the greatest depth
            let mut best_dep: Option<&str> = None;
            let mut best_depth = 0;

            for dep_id in &task.depends_on {
                if visited.contains(dep_id.as_str()) {
                    continue;
                }
                if !task_map.contains_key(dep_id.as_str()) {
                    continue;
                }
                // Compute depth for this dep
                let mut memo = HashMap::new();
                let depth = self.compute_depth(dep_id, task_map, &mut memo, &mut HashSet::new());
                if depth >= best_depth {
                    best_depth = depth;
                    best_dep = Some(dep_id.as_str());
                }
            }

            match best_dep {
                Some(dep) => {
                    visited.insert(dep);
                    chain.push(dep.to_string());
                    current = dep;
                }
                None => break,
            }
        }

        chain.reverse();
        chain
    }

    /// Count connected components using BFS (treating deps as undirected edges).
    fn count_connected_components(&self, tasks: &[TaskDepData], task_ids: &HashSet<&str>) -> usize {
        if tasks.is_empty() {
            return 0;
        }

        // Build undirected adjacency
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for task in tasks {
            adj.entry(task.id.as_str()).or_default();
            for dep_id in &task.depends_on {
                if task_ids.contains(dep_id.as_str()) {
                    adj.entry(task.id.as_str())
                        .or_default()
                        .push(dep_id.as_str());
                    adj.entry(dep_id.as_str())
                        .or_default()
                        .push(task.id.as_str());
                }
            }
        }

        let mut visited: HashSet<&str> = HashSet::new();
        let mut components = 0;

        for task in tasks {
            if visited.contains(task.id.as_str()) {
                continue;
            }
            components += 1;
            // BFS from this task
            let mut queue = VecDeque::new();
            queue.push_back(task.id.as_str());
            visited.insert(task.id.as_str());

            while let Some(current) = queue.pop_front() {
                if let Some(neighbors) = adj.get(current) {
                    for &neighbor in neighbors {
                        if visited.insert(neighbor) {
                            queue.push_back(neighbor);
                        }
                    }
                }
            }
        }

        components
    }

    /// Get the dependency chain for a specific task (all transitive dependencies).
    pub fn get_dependency_chain(&self, task_id: &str, tasks: &[TaskDepData]) -> Vec<String> {
        let task_map: HashMap<&str, &TaskDepData> =
            tasks.iter().map(|t| (t.id.as_str(), t)).collect();
        let task_ids: HashSet<&str> = tasks.iter().map(|t| t.id.as_str()).collect();

        let mut result = Vec::new();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        queue.push_back(task_id);
        visited.insert(task_id);

        while let Some(current) = queue.pop_front() {
            if let Some(task) = task_map.get(current) {
                for dep_id in &task.depends_on {
                    if task_ids.contains(dep_id.as_str()) && visited.insert(dep_id.as_str()) {
                        result.push(dep_id.clone());
                        queue.push_back(dep_id.as_str());
                    }
                }
            }
        }

        result
    }

    /// Get all tasks that directly or transitively depend on a given task.
    pub fn get_dependents(&self, task_id: &str, tasks: &[TaskDepData]) -> Vec<String> {
        // Build reverse adjacency: task → tasks that depend on it
        let mut reverse_adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for task in tasks {
            for dep_id in &task.depends_on {
                reverse_adj
                    .entry(dep_id.as_str())
                    .or_default()
                    .push(task.id.as_str());
            }
        }

        let mut result = Vec::new();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        queue.push_back(task_id);
        visited.insert(task_id);

        while let Some(current) = queue.pop_front() {
            if let Some(dependents) = reverse_adj.get(current) {
                for &dep in dependents {
                    if visited.insert(dep) {
                        result.push(dep.to_string());
                        queue.push_back(dep);
                    }
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task(id: &str, deps: Vec<&str>) -> TaskDepData {
        TaskDepData {
            id: id.to_string(),
            depends_on: deps.into_iter().map(String::from).collect(),
            is_complete: false,
            is_error: false,
        }
    }

    fn make_task_with_state(id: &str, deps: Vec<&str>, complete: bool, error: bool) -> TaskDepData {
        TaskDepData {
            id: id.to_string(),
            depends_on: deps.into_iter().map(String::from).collect(),
            is_complete: complete,
            is_error: error,
        }
    }

    #[test]
    fn test_empty_graph() {
        let validator = DependencyGraphValidator::new();
        let result = validator.validate(&[]);
        assert!(result.is_valid);
        assert!(result.issues.is_empty());
    }

    #[test]
    fn test_simple_valid_graph() {
        let validator = DependencyGraphValidator::new();
        let tasks = vec![
            make_task("a", vec![]),
            make_task("b", vec!["a"]),
            make_task("c", vec!["b"]),
        ];
        let result = validator.validate(&tasks);
        assert!(result.is_valid);
        assert_eq!(result.error_count, 0);
    }

    #[test]
    fn test_orphaned_dependency() {
        let validator = DependencyGraphValidator::new();
        let tasks = vec![make_task("a", vec!["nonexistent"])];
        let result = validator.validate(&tasks);
        assert!(!result.is_valid);
        assert_eq!(result.error_count, 1);
        assert_eq!(result.issues[0].category, IssueCategory::OrphanedDependency);
    }

    #[test]
    fn test_self_dependency() {
        let validator = DependencyGraphValidator::new();
        let tasks = vec![make_task("a", vec!["a"])];
        let result = validator.validate(&tasks);
        assert!(!result.is_valid);
        assert_eq!(result.error_count, 1);
        assert_eq!(result.issues[0].category, IssueCategory::SelfDependency);
    }

    #[test]
    fn test_circular_dependency() {
        let validator = DependencyGraphValidator::new();
        let tasks = vec![
            make_task("a", vec!["b"]),
            make_task("b", vec!["c"]),
            make_task("c", vec!["a"]),
        ];
        let result = validator.validate(&tasks);
        assert!(!result.is_valid);
        assert!(
            result
                .issues
                .iter()
                .any(|i| i.category == IssueCategory::CircularDependency)
        );
    }

    #[test]
    fn test_blocked_by_failure() {
        let validator = DependencyGraphValidator::new();
        let tasks = vec![
            make_task_with_state("a", vec![], false, true),
            make_task("b", vec!["a"]),
        ];
        let result = validator.validate(&tasks);
        assert!(result.is_valid); // Warnings don't make it invalid
        assert_eq!(result.warning_count, 1);
        assert_eq!(result.issues[0].category, IssueCategory::BlockedByFailure);
    }

    #[test]
    fn test_redundant_dependency() {
        let validator = DependencyGraphValidator::new();
        // A depends on B and C; B also depends on C → A→C is redundant
        let tasks = vec![
            make_task("c", vec![]),
            make_task("b", vec!["c"]),
            make_task("a", vec!["b", "c"]),
        ];
        let result = validator.validate(&tasks);
        assert!(result.is_valid);
        assert!(
            result
                .issues
                .iter()
                .any(|i| i.category == IssueCategory::RedundantDependency)
        );
    }

    #[test]
    fn test_excessive_depth() {
        let mut config = DependencyGraphConfig::default();
        config.max_depth_warning_threshold = 2;
        let validator = DependencyGraphValidator::with_config(config);

        // Chain: a → b → c → d (depth 3 for 'a')
        let tasks = vec![
            make_task("d", vec![]),
            make_task("c", vec!["d"]),
            make_task("b", vec!["c"]),
            make_task("a", vec!["b"]),
        ];
        let result = validator.validate(&tasks);
        assert!(result.is_valid);
        assert!(
            result
                .issues
                .iter()
                .any(|i| i.category == IssueCategory::ExcessiveDepth)
        );
    }

    #[test]
    fn test_topological_sort_simple() {
        let validator = DependencyGraphValidator::new();
        let tasks = vec![
            make_task("c", vec!["b"]),
            make_task("a", vec![]),
            make_task("b", vec!["a"]),
        ];
        let order = validator.topological_sort(&tasks);
        assert!(order.is_complete);
        assert_eq!(order.order.len(), 3);

        // 'a' must come before 'b', 'b' must come before 'c'
        let pos_a = order.order.iter().position(|x| x == "a").unwrap();
        let pos_b = order.order.iter().position(|x| x == "b").unwrap();
        let pos_c = order.order.iter().position(|x| x == "c").unwrap();
        assert!(pos_a < pos_b);
        assert!(pos_b < pos_c);
    }

    #[test]
    fn test_topological_sort_with_cycle() {
        let validator = DependencyGraphValidator::new();
        let tasks = vec![
            make_task("a", vec!["b"]),
            make_task("b", vec!["a"]),
            make_task("c", vec![]),
        ];
        let order = validator.topological_sort(&tasks);
        assert!(!order.is_complete);
        assert!(!order.unorderable.is_empty());
        // 'c' should still be orderable
        assert!(order.order.contains(&"c".to_string()));
    }

    #[test]
    fn test_graph_stats_empty() {
        let validator = DependencyGraphValidator::new();
        let stats = validator.compute_stats(&[]);
        assert_eq!(stats.total_tasks, 0);
        assert_eq!(stats.total_edges, 0);
        assert_eq!(stats.connected_components, 0);
    }

    #[test]
    fn test_graph_stats_linear_chain() {
        let validator = DependencyGraphValidator::new();
        let tasks = vec![
            make_task("a", vec![]),
            make_task("b", vec!["a"]),
            make_task("c", vec!["b"]),
        ];
        let stats = validator.compute_stats(&tasks);
        assert_eq!(stats.total_tasks, 3);
        assert_eq!(stats.tasks_with_deps, 2);
        assert_eq!(stats.root_tasks, 1);
        assert_eq!(stats.total_edges, 2);
        assert_eq!(stats.max_chain_depth, 2);
        assert_eq!(stats.connected_components, 1);
    }

    #[test]
    fn test_graph_stats_independent_tasks() {
        let validator = DependencyGraphValidator::new();
        let tasks = vec![
            make_task("a", vec![]),
            make_task("b", vec![]),
            make_task("c", vec![]),
        ];
        let stats = validator.compute_stats(&tasks);
        assert_eq!(stats.total_tasks, 3);
        assert_eq!(stats.tasks_with_deps, 0);
        assert_eq!(stats.root_tasks, 3);
        assert_eq!(stats.total_edges, 0);
        assert_eq!(stats.max_chain_depth, 0);
        assert_eq!(stats.connected_components, 3);
    }

    #[test]
    fn test_graph_stats_diamond() {
        let validator = DependencyGraphValidator::new();
        // Diamond: a → b, a → c, b → d, c → d
        let tasks = vec![
            make_task("d", vec![]),
            make_task("b", vec!["d"]),
            make_task("c", vec!["d"]),
            make_task("a", vec!["b", "c"]),
        ];
        let stats = validator.compute_stats(&tasks);
        assert_eq!(stats.total_tasks, 4);
        assert_eq!(stats.tasks_with_deps, 3);
        assert_eq!(stats.root_tasks, 1);
        assert_eq!(stats.total_edges, 4);
        assert_eq!(stats.connected_components, 1);
    }

    #[test]
    fn test_get_dependency_chain() {
        let validator = DependencyGraphValidator::new();
        let tasks = vec![
            make_task("a", vec![]),
            make_task("b", vec!["a"]),
            make_task("c", vec!["b"]),
            make_task("d", vec!["c"]),
        ];
        let chain = validator.get_dependency_chain("d", &tasks);
        assert_eq!(chain.len(), 3);
        assert!(chain.contains(&"a".to_string()));
        assert!(chain.contains(&"b".to_string()));
        assert!(chain.contains(&"c".to_string()));
    }

    #[test]
    fn test_get_dependents() {
        let validator = DependencyGraphValidator::new();
        let tasks = vec![
            make_task("a", vec![]),
            make_task("b", vec!["a"]),
            make_task("c", vec!["a"]),
            make_task("d", vec!["b"]),
        ];
        let dependents = validator.get_dependents("a", &tasks);
        assert_eq!(dependents.len(), 3);
        assert!(dependents.contains(&"b".to_string()));
        assert!(dependents.contains(&"c".to_string()));
        assert!(dependents.contains(&"d".to_string()));
    }

    #[test]
    fn test_validation_result_format_summary_valid() {
        let result = ValidationResult {
            is_valid: true,
            issues: vec![],
            error_count: 0,
            warning_count: 0,
            info_count: 0,
        };
        let summary = result.format_summary();
        assert!(summary.contains("✅"));
        assert!(summary.contains("valid"));
    }

    #[test]
    fn test_validation_result_format_summary_with_issues() {
        let result = ValidationResult {
            is_valid: false,
            issues: vec![DependencyIssue {
                task_id: "task1".to_string(),
                category: IssueCategory::OrphanedDependency,
                severity: IssueSeverity::Error,
                message: "Dependency 'missing' does not exist".to_string(),
                related_ids: vec!["missing".to_string()],
            }],
            error_count: 1,
            warning_count: 0,
            info_count: 0,
        };
        let summary = result.format_summary();
        assert!(summary.contains("❌"));
        assert!(summary.contains("task1"));
        assert!(summary.contains("missing"));
    }

    #[test]
    fn test_graph_stats_format_summary() {
        let stats = GraphStats {
            total_tasks: 5,
            tasks_with_deps: 3,
            root_tasks: 2,
            depended_upon_count: 2,
            total_edges: 4,
            max_chain_depth: 3,
            connected_components: 1,
            avg_deps_per_task: 1.33,
            longest_chain: vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
            ],
        };
        let summary = stats.format_summary();
        assert!(summary.contains("Total tasks: 5"));
        assert!(summary.contains("Max chain depth: 3"));
        assert!(summary.contains("a → b → c → d"));
    }

    #[test]
    fn test_topological_order_format() {
        let order = TopologicalOrder {
            order: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            is_complete: true,
            unorderable: vec![],
        };
        let formatted = order.format_order();
        assert!(formatted.contains("1. a"));
        assert!(formatted.contains("2. b"));
        assert!(formatted.contains("3. c"));
    }

    #[test]
    fn test_config_default() {
        let config = DependencyGraphConfig::default();
        assert!(config.auto_validate);
        assert_eq!(config.max_depth_warning_threshold, 10);
        assert!(config.include_completed_in_analysis);
    }

    #[test]
    fn test_set_config() {
        let mut validator = DependencyGraphValidator::new();
        let mut config = DependencyGraphConfig::default();
        config.auto_validate = false;
        config.max_depth_warning_threshold = 5;
        validator.set_config(config);
        assert!(!validator.config().auto_validate);
        assert_eq!(validator.config().max_depth_warning_threshold, 5);
    }

    #[test]
    fn test_multiple_orphaned_deps() {
        let validator = DependencyGraphValidator::new();
        let tasks = vec![make_task("a", vec!["x", "y", "z"])];
        let result = validator.validate(&tasks);
        assert!(!result.is_valid);
        assert_eq!(result.error_count, 3);
        assert!(
            result
                .issues
                .iter()
                .all(|i| i.category == IssueCategory::OrphanedDependency)
        );
    }

    #[test]
    fn test_diamond_no_redundancy() {
        // Diamond: a → b, a → c, b → d, c → d
        // Here a→b is NOT redundant (b is not transitively reachable from c)
        // and a→c is NOT redundant (c is not transitively reachable from b)
        let validator = DependencyGraphValidator::new();
        let tasks = vec![
            make_task("d", vec![]),
            make_task("b", vec!["d"]),
            make_task("c", vec!["d"]),
            make_task("a", vec!["b", "c"]),
        ];
        let result = validator.validate(&tasks);
        assert!(result.is_valid);
        // No redundant deps in a diamond
        assert!(
            !result
                .issues
                .iter()
                .any(|i| i.category == IssueCategory::RedundantDependency)
        );
    }

    #[test]
    fn test_get_dependency_chain_no_deps() {
        let validator = DependencyGraphValidator::new();
        let tasks = vec![make_task("a", vec![])];
        let chain = validator.get_dependency_chain("a", &tasks);
        assert!(chain.is_empty());
    }

    #[test]
    fn test_get_dependents_leaf_task() {
        let validator = DependencyGraphValidator::new();
        let tasks = vec![make_task("a", vec![]), make_task("b", vec!["a"])];
        let dependents = validator.get_dependents("b", &tasks);
        assert!(dependents.is_empty());
    }

    #[test]
    fn test_longest_chain() {
        let validator = DependencyGraphValidator::new();
        let tasks = vec![
            make_task("a", vec![]),
            make_task("b", vec!["a"]),
            make_task("c", vec!["b"]),
            make_task("d", vec!["c"]),
            make_task("x", vec![]),
            make_task("y", vec!["x"]),
        ];
        let stats = validator.compute_stats(&tasks);
        assert_eq!(stats.max_chain_depth, 3);
        assert_eq!(stats.longest_chain.len(), 4); // a → b → c → d
        assert_eq!(stats.longest_chain[0], "a");
        assert_eq!(stats.longest_chain[3], "d");
    }

    #[test]
    fn test_avg_deps_per_task() {
        let validator = DependencyGraphValidator::new();
        let tasks = vec![
            make_task("a", vec![]),
            make_task("b", vec!["a"]),
            make_task("c", vec!["a", "b"]),
        ];
        let stats = validator.compute_stats(&tasks);
        // tasks_with_deps = 2 (b has 1 dep, c has 2 deps), total_edges = 3
        assert_eq!(stats.tasks_with_deps, 2);
        assert!((stats.avg_deps_per_task - 1.5).abs() < 0.01);
    }
}
