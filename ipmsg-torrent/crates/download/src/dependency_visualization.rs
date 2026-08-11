//! Download Task Dependency Graph Visualization System
//!
//! This module provides visualization and analysis capabilities for task dependency graphs.
//! It helps users understand complex task relationships, detect cycles, and optimize
//! task execution order.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

/// Represents a node in the dependency graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DependencyNode {
    /// Task ID
    pub task_id: String,
    /// Task name (for display)
    pub task_name: String,
    /// Dependencies (tasks this task depends on)
    pub dependencies: Vec<String>,
    /// Dependents (tasks that depend on this task)
    pub dependents: Vec<String>,
    /// Task state (e.g., "Running", "Queued", "Completed")
    pub state: String,
    /// Depth level in the dependency tree (0 = root)
    pub depth: usize,
    /// Whether this task is part of a cycle
    pub in_cycle: bool,
}

/// Represents the entire dependency graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DependencyGraph {
    /// All nodes in the graph
    pub nodes: HashMap<String, DependencyNode>,
    /// Detected cycles (each cycle is a list of task IDs)
    pub cycles: Vec<Vec<String>>,
    /// Root nodes (tasks with no dependencies)
    pub roots: Vec<String>,
    /// Leaf nodes (tasks with no dependents)
    pub leaves: Vec<String>,
    /// Maximum depth of the graph
    pub max_depth: usize,
    /// Graph statistics
    pub stats: GraphStats,
}

/// Statistics about the dependency graph
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GraphStats {
    /// Total number of tasks
    pub total_tasks: usize,
    /// Number of root tasks (no dependencies)
    pub root_tasks: usize,
    /// Number of leaf tasks (no dependents)
    pub leaf_tasks: usize,
    /// Number of tasks in cycles
    pub cyclic_tasks: usize,
    /// Number of isolated tasks (no dependencies or dependents)
    pub isolated_tasks: usize,
    /// Average number of dependencies per task
    pub avg_dependencies: f64,
    /// Average number of dependents per task
    pub avg_dependents: f64,
    /// Maximum depth of the dependency tree
    pub max_depth: usize,
    /// Number of connected components
    pub connected_components: usize,
}

/// Configuration for the visualization system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VisualizationConfig {
    /// Whether to include completed tasks in the graph
    pub include_completed: bool,
    /// Whether to highlight cycles
    pub highlight_cycles: bool,
    /// Maximum depth to display (0 = unlimited)
    pub max_display_depth: usize,
    /// Whether to show task names or just IDs
    pub show_task_names: bool,
    /// Whether to compute statistics
    pub compute_stats: bool,
}

impl Default for VisualizationConfig {
    fn default() -> Self {
        Self {
            include_completed: true,
            highlight_cycles: true,
            max_display_depth: 0,
            show_task_names: true,
            compute_stats: true,
        }
    }
}

/// Manager for dependency graph visualization
#[derive(Debug, Default)]
pub struct DependencyVisualizationManager {
    /// Current graph
    graph: Option<DependencyGraph>,
    /// Configuration
    config: VisualizationConfig,
}

impl DependencyVisualizationManager {
    /// Create a new visualization manager
    pub fn new() -> Self {
        Self {
            graph: None,
            config: VisualizationConfig::default(),
        }
    }

    /// Create with configuration
    pub fn with_config(config: VisualizationConfig) -> Self {
        Self {
            graph: None,
            config,
        }
    }

    /// Build dependency graph from task data
    pub fn build_graph(
        &mut self,
        tasks: &[(String, String, String, Vec<String>)], // (id, name, state, dependencies)
    ) {
        let mut nodes = HashMap::new();
        let mut cycles = Vec::new();

        // Create nodes
        for (task_id, task_name, state, dependencies) in tasks {
            if !self.config.include_completed && state == "Completed" {
                continue;
            }

            nodes.insert(
                task_id.clone(),
                DependencyNode {
                    task_id: task_id.clone(),
                    task_name: task_name.clone(),
                    dependencies: dependencies.clone(),
                    dependents: Vec::new(),
                    state: state.clone(),
                    depth: 0,
                    in_cycle: false,
                },
            );
        }

        // Build dependents relationships
        for (task_id, _, _, dependencies) in tasks {
            if !nodes.contains_key(task_id) {
                continue;
            }

            for dep_id in dependencies {
                if let Some(node) = nodes.get_mut(dep_id)
                    && !node.dependents.contains(task_id)
                {
                    node.dependents.push(task_id.clone());
                }
            }
        }

        // Detect cycles using DFS
        if self.config.highlight_cycles {
            cycles = self.detect_cycles(&nodes);
            // Mark nodes in cycles
            for cycle in &cycles {
                for task_id in cycle {
                    if let Some(node) = nodes.get_mut(task_id) {
                        node.in_cycle = true;
                    }
                }
            }
        }

        // Calculate depths
        self.calculate_depths(&mut nodes);

        // Find roots and leaves
        let roots: Vec<String> = nodes
            .values()
            .filter(|n| n.dependencies.is_empty())
            .map(|n| n.task_id.clone())
            .collect();

        let leaves: Vec<String> = nodes
            .values()
            .filter(|n| n.dependents.is_empty())
            .map(|n| n.task_id.clone())
            .collect();

        let max_depth = nodes.values().map(|n| n.depth).max().unwrap_or(0);

        // Calculate statistics
        let stats = if self.config.compute_stats {
            self.calculate_stats(&nodes, &cycles)
        } else {
            GraphStats::default()
        };

        self.graph = Some(DependencyGraph {
            nodes,
            cycles,
            roots,
            leaves,
            max_depth,
            stats,
        });
    }

    /// Detect cycles using DFS
    fn detect_cycles(&self, nodes: &HashMap<String, DependencyNode>) -> Vec<Vec<String>> {
        let mut cycles = Vec::new();
        let mut visited = HashSet::new();
        let mut recursion_stack = HashSet::new();

        for task_id in nodes.keys() {
            if !visited.contains(task_id) {
                self.dfs_cycle(
                    task_id,
                    nodes,
                    &mut visited,
                    &mut recursion_stack,
                    &mut Vec::new(),
                    &mut cycles,
                );
            }
        }

        cycles
    }

    /// DFS helper for cycle detection
    fn dfs_cycle(
        &self,
        task_id: &str,
        nodes: &HashMap<String, DependencyNode>,
        visited: &mut HashSet<String>,
        recursion_stack: &mut HashSet<String>,
        path: &mut Vec<String>,
        cycles: &mut Vec<Vec<String>>,
    ) {
        visited.insert(task_id.to_string());
        recursion_stack.insert(task_id.to_string());
        path.push(task_id.to_string());

        if let Some(node) = nodes.get(task_id) {
            for dep_id in &node.dependencies {
                if !nodes.contains_key(dep_id) {
                    continue;
                }

                if !visited.contains(dep_id) {
                    self.dfs_cycle(dep_id, nodes, visited, recursion_stack, path, cycles);
                } else if recursion_stack.contains(dep_id) {
                    // Found a cycle
                    if let Some(cycle_start) = path.iter().position(|id| id == dep_id) {
                        let cycle = path[cycle_start..].to_vec();
                        if cycle.len() > 1 {
                            cycles.push(cycle);
                        }
                    }
                }
            }
        }

        path.pop();
        recursion_stack.remove(task_id);
    }

    /// Calculate depth for each node using BFS
    fn calculate_depths(&self, nodes: &mut HashMap<String, DependencyNode>) {
        // Find roots
        let roots: Vec<String> = nodes
            .values()
            .filter(|n| n.dependencies.is_empty())
            .map(|n| n.task_id.clone())
            .collect();

        // BFS from roots
        let mut queue: VecDeque<(String, usize)> = VecDeque::new();
        for root in &roots {
            queue.push_back((root.clone(), 0));
        }

        let mut visited = HashSet::new();

        while let Some((task_id, depth)) = queue.pop_front() {
            if visited.contains(&task_id) {
                continue;
            }
            visited.insert(task_id.clone());

            if let Some(node) = nodes.get_mut(&task_id) {
                node.depth = depth;

                // Add dependents to queue
                for dependent in &node.dependents {
                    if !visited.contains(dependent) {
                        queue.push_back((dependent.clone(), depth + 1));
                    }
                }
            }
        }
    }

    /// Calculate graph statistics
    fn calculate_stats(
        &self,
        nodes: &HashMap<String, DependencyNode>,
        cycles: &[Vec<String>],
    ) -> GraphStats {
        let total_tasks = nodes.len();
        let root_tasks = nodes.values().filter(|n| n.dependencies.is_empty()).count();
        let leaf_tasks = nodes.values().filter(|n| n.dependents.is_empty()).count();
        let cyclic_tasks: HashSet<String> = cycles.iter().flatten().cloned().collect();
        let cyclic_tasks = cyclic_tasks.len();
        let isolated_tasks = nodes
            .values()
            .filter(|n| n.dependencies.is_empty() && n.dependents.is_empty())
            .count();

        let total_deps: usize = nodes.values().map(|n| n.dependencies.len()).sum();
        let total_dependents: usize = nodes.values().map(|n| n.dependents.len()).sum();

        let avg_dependencies = if total_tasks > 0 {
            total_deps as f64 / total_tasks as f64
        } else {
            0.0
        };

        let avg_dependents = if total_tasks > 0 {
            total_dependents as f64 / total_tasks as f64
        } else {
            0.0
        };

        let max_depth = nodes.values().map(|n| n.depth).max().unwrap_or(0);

        // Calculate connected components using union-find
        let connected_components = self.count_connected_components(nodes);

        GraphStats {
            total_tasks,
            root_tasks,
            leaf_tasks,
            cyclic_tasks,
            isolated_tasks,
            avg_dependencies,
            avg_dependents,
            max_depth,
            connected_components,
        }
    }

    /// Count connected components using union-find
    fn count_connected_components(&self, nodes: &HashMap<String, DependencyNode>) -> usize {
        let mut parent: HashMap<String, String> = HashMap::new();
        let mut rank: HashMap<String, usize> = HashMap::new();

        // Initialize
        for task_id in nodes.keys() {
            parent.insert(task_id.clone(), task_id.clone());
            rank.insert(task_id.clone(), 0);
        }

        // Union
        for (task_id, node) in nodes {
            for dep_id in &node.dependencies {
                if nodes.contains_key(dep_id) {
                    self.union(task_id, dep_id, &mut parent, &mut rank);
                }
            }
        }

        // Count unique roots
        let mut roots = HashSet::new();
        for task_id in nodes.keys() {
            roots.insert(self.find(task_id, &mut parent));
        }

        roots.len()
    }

    /// Find with path compression
    fn find(&self, task_id: &str, parent: &mut HashMap<String, String>) -> String {
        let current = parent.get(task_id).unwrap().clone();
        if current != task_id {
            let root = self.find(&current, parent);
            parent.insert(task_id.to_string(), root.clone());
        }
        current
    }

    /// Union by rank
    fn union(
        &self,
        x: &str,
        y: &str,
        parent: &mut HashMap<String, String>,
        rank: &mut HashMap<String, usize>,
    ) {
        let x_root = self.find(x, parent);
        let y_root = self.find(y, parent);

        if x_root == y_root {
            return;
        }

        let x_rank = *rank.get(&x_root).unwrap_or(&0);
        let y_rank = *rank.get(&y_root).unwrap_or(&0);

        if x_rank < y_rank {
            parent.insert(x_root, y_root);
        } else if x_rank > y_rank {
            parent.insert(y_root, x_root);
        } else {
            parent.insert(y_root, x_root.clone());
            *rank.get_mut(&x_root).unwrap() += 1;
        }
    }

    /// Get the current graph
    pub fn get_graph(&self) -> Option<&DependencyGraph> {
        self.graph.as_ref()
    }

    /// Get graph statistics
    pub fn get_stats(&self) -> Option<&GraphStats> {
        self.graph.as_ref().map(|g| &g.stats)
    }

    /// Get detected cycles
    pub fn get_cycles(&self) -> Option<&[Vec<String>]> {
        self.graph.as_ref().map(|g| g.cycles.as_slice())
    }

    /// Get root tasks
    pub fn get_roots(&self) -> Option<&[String]> {
        self.graph.as_ref().map(|g| g.roots.as_slice())
    }

    /// Get leaf tasks
    pub fn get_leaves(&self) -> Option<&[String]> {
        self.graph.as_ref().map(|g| g.leaves.as_slice())
    }

    /// Get a specific node
    pub fn get_node(&self, task_id: &str) -> Option<&DependencyNode> {
        self.graph.as_ref().and_then(|g| g.nodes.get(task_id))
    }

    /// Get configuration
    pub fn get_config(&self) -> &VisualizationConfig {
        &self.config
    }

    /// Set configuration
    pub fn set_config(&mut self, config: VisualizationConfig) {
        self.config = config;
    }

    /// Generate a text-based visualization of the graph
    pub fn visualize_text(&self) -> Option<String> {
        let graph = self.graph.as_ref()?;
        let mut output = Vec::new();

        output.push("=== Dependency Graph Visualization ===".to_string());
        output.push(format!("Total Tasks: {}", graph.stats.total_tasks));
        output.push(format!("Root Tasks: {}", graph.stats.root_tasks));
        output.push(format!("Leaf Tasks: {}", graph.stats.leaf_tasks));
        output.push(format!("Max Depth: {}", graph.max_depth));
        output.push(format!("Cyclic Tasks: {}", graph.stats.cyclic_tasks));
        output.push(format!(
            "Connected Components: {}",
            graph.stats.connected_components
        ));
        output.push(String::new());

        // Show cycles if any
        if !graph.cycles.is_empty() {
            output.push("⚠️  Detected Cycles:".to_string());
            for (i, cycle) in graph.cycles.iter().enumerate() {
                output.push(format!("  Cycle {}: {}", i + 1, cycle.join(" → ")));
            }
            output.push(String::new());
        }

        // Show roots
        if !graph.roots.is_empty() {
            output.push("🌱 Root Tasks (no dependencies):".to_string());
            for root in &graph.roots {
                if let Some(node) = graph.nodes.get(root) {
                    output.push(format!("  • {} ({})", node.task_name, root));
                }
            }
            output.push(String::new());
        }

        // Show leaves
        if !graph.leaves.is_empty() {
            output.push("🍃 Leaf Tasks (no dependents):".to_string());
            for leaf in &graph.leaves {
                if let Some(node) = graph.nodes.get(leaf) {
                    output.push(format!("  • {} ({})", node.task_name, leaf));
                }
            }
            output.push(String::new());
        }

        // Show dependency tree
        output.push("📊 Dependency Tree:".to_string());
        for root in &graph.roots {
            self.print_tree(root, &graph.nodes, &mut output, 0, &mut HashSet::new());
        }

        Some(output.join("\n"))
    }

    /// Recursive helper to print tree structure
    fn print_tree(
        &self,
        task_id: &str,
        nodes: &HashMap<String, DependencyNode>,
        output: &mut Vec<String>,
        indent: usize,
        visited: &mut HashSet<String>,
    ) {
        if visited.contains(task_id) {
            return;
        }
        visited.insert(task_id.to_string());

        if let Some(node) = nodes.get(task_id) {
            let prefix = "  ".repeat(indent);
            let marker = if node.in_cycle { "⚠️ " } else { "" };
            let state_marker = match node.state.as_str() {
                "Completed" => "✅",
                "Running" => "🔄",
                "Queued" => "⏳",
                _ => "⏸️",
            };

            output.push(format!(
                "{}{}{} {} ({})",
                prefix, marker, state_marker, node.task_name, task_id
            ));

            for dependent in &node.dependents {
                self.print_tree(dependent, nodes, output, indent + 1, visited);
            }
        }
    }

    /// Export graph to DOT format (for Graphviz)
    pub fn export_dot(&self) -> Option<String> {
        let graph = self.graph.as_ref()?;
        let mut output = vec![
            "digraph DependencyGraph {".to_string(),
            "  rankdir=TB;".to_string(),
            "  node [shape=box];".to_string(),
            String::new(),
        ];

        // Nodes
        for node in graph.nodes.values() {
            let color = if node.in_cycle {
                "red"
            } else {
                match node.state.as_str() {
                    "Completed" => "green",
                    "Running" => "blue",
                    "Queued" => "yellow",
                    _ => "gray",
                }
            };

            let label = if self.config.show_task_names {
                format!("{}\\n{}", node.task_name, node.task_id)
            } else {
                node.task_id.clone()
            };

            output.push(format!(
                "  \"{}\" [label=\"{}\", color={}];",
                node.task_id, label, color
            ));
        }

        output.push(String::new());

        // Edges
        for node in graph.nodes.values() {
            for dep_id in &node.dependencies {
                if graph.nodes.contains_key(dep_id) {
                    output.push(format!("  \"{}\" -> \"{}\";", dep_id, node.task_id));
                }
            }
        }

        output.push("}".to_string());

        Some(output.join("\n"))
    }

    /// Clear the graph
    pub fn clear(&mut self) {
        self.graph = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_graph() {
        let mut manager = DependencyVisualizationManager::new();
        let tasks = vec![
            (
                "task1".to_string(),
                "Task 1".to_string(),
                "Running".to_string(),
                vec![],
            ),
            (
                "task2".to_string(),
                "Task 2".to_string(),
                "Queued".to_string(),
                vec!["task1".to_string()],
            ),
            (
                "task3".to_string(),
                "Task 3".to_string(),
                "Queued".to_string(),
                vec!["task2".to_string()],
            ),
        ];

        manager.build_graph(&tasks);

        let graph = manager.get_graph().unwrap();
        assert_eq!(graph.stats.total_tasks, 3);
        assert_eq!(graph.stats.root_tasks, 1);
        assert_eq!(graph.stats.leaf_tasks, 1);
        assert_eq!(graph.max_depth, 2);
    }

    #[test]
    fn test_cycle_detection() {
        let mut manager = DependencyVisualizationManager::new();
        let tasks = vec![
            (
                "task1".to_string(),
                "Task 1".to_string(),
                "Running".to_string(),
                vec!["task3".to_string()],
            ),
            (
                "task2".to_string(),
                "Task 2".to_string(),
                "Queued".to_string(),
                vec!["task1".to_string()],
            ),
            (
                "task3".to_string(),
                "Task 3".to_string(),
                "Queued".to_string(),
                vec!["task2".to_string()],
            ),
        ];

        manager.build_graph(&tasks);

        let graph = manager.get_graph().unwrap();
        assert!(!graph.cycles.is_empty());
        assert_eq!(graph.stats.cyclic_tasks, 3);
    }

    #[test]
    fn test_multiple_roots() {
        let mut manager = DependencyVisualizationManager::new();
        let tasks = vec![
            (
                "task1".to_string(),
                "Task 1".to_string(),
                "Running".to_string(),
                vec![],
            ),
            (
                "task2".to_string(),
                "Task 2".to_string(),
                "Running".to_string(),
                vec![],
            ),
            (
                "task3".to_string(),
                "Task 3".to_string(),
                "Queued".to_string(),
                vec!["task1".to_string(), "task2".to_string()],
            ),
        ];

        manager.build_graph(&tasks);

        let graph = manager.get_graph().unwrap();
        assert_eq!(graph.stats.root_tasks, 2);
        assert_eq!(graph.roots.len(), 2);
    }

    #[test]
    fn test_isolated_tasks() {
        let mut manager = DependencyVisualizationManager::new();
        let tasks = vec![
            (
                "task1".to_string(),
                "Task 1".to_string(),
                "Running".to_string(),
                vec![],
            ),
            (
                "task2".to_string(),
                "Task 2".to_string(),
                "Running".to_string(),
                vec![],
            ),
            (
                "task3".to_string(),
                "Task 3".to_string(),
                "Queued".to_string(),
                vec!["task1".to_string()],
            ),
        ];

        manager.build_graph(&tasks);

        let graph = manager.get_graph().unwrap();
        assert_eq!(graph.stats.isolated_tasks, 1); // task2 is isolated
    }

    #[test]
    fn test_text_visualization() {
        let mut manager = DependencyVisualizationManager::new();
        let tasks = vec![
            (
                "task1".to_string(),
                "Task 1".to_string(),
                "Running".to_string(),
                vec![],
            ),
            (
                "task2".to_string(),
                "Task 2".to_string(),
                "Queued".to_string(),
                vec!["task1".to_string()],
            ),
        ];

        manager.build_graph(&tasks);

        let viz = manager.visualize_text().unwrap();
        assert!(viz.contains("Dependency Graph Visualization"));
        assert!(viz.contains("Total Tasks: 2"));
    }

    #[test]
    fn test_dot_export() {
        let mut manager = DependencyVisualizationManager::new();
        let tasks = vec![
            (
                "task1".to_string(),
                "Task 1".to_string(),
                "Running".to_string(),
                vec![],
            ),
            (
                "task2".to_string(),
                "Task 2".to_string(),
                "Queued".to_string(),
                vec!["task1".to_string()],
            ),
        ];

        manager.build_graph(&tasks);

        let dot = manager.export_dot().unwrap();
        assert!(dot.contains("digraph DependencyGraph"));
        assert!(dot.contains("task1"));
        assert!(dot.contains("task2"));
    }

    #[test]
    fn test_config() {
        let mut manager = DependencyVisualizationManager::new();
        let config = VisualizationConfig {
            include_completed: false,
            highlight_cycles: true,
            max_display_depth: 5,
            show_task_names: false,
            compute_stats: true,
        };

        manager.set_config(config);
        let retrieved = manager.get_config();
        assert!(!retrieved.include_completed);
        assert_eq!(retrieved.max_display_depth, 5);
    }

    #[test]
    fn test_empty_graph() {
        let mut manager = DependencyVisualizationManager::new();
        let tasks: Vec<(String, String, String, Vec<String>)> = vec![];

        manager.build_graph(&tasks);

        let graph = manager.get_graph().unwrap();
        assert_eq!(graph.stats.total_tasks, 0);
        assert!(graph.roots.is_empty());
        assert!(graph.leaves.is_empty());
    }

    #[test]
    fn test_get_node() {
        let mut manager = DependencyVisualizationManager::new();
        let tasks = vec![(
            "task1".to_string(),
            "Task 1".to_string(),
            "Running".to_string(),
            vec![],
        )];

        manager.build_graph(&tasks);

        let node = manager.get_node("task1").unwrap();
        assert_eq!(node.task_id, "task1");
        assert_eq!(node.task_name, "Task 1");
    }

    #[test]
    fn test_connected_components() {
        let mut manager = DependencyVisualizationManager::new();
        let tasks = vec![
            (
                "task1".to_string(),
                "Task 1".to_string(),
                "Running".to_string(),
                vec![],
            ),
            (
                "task2".to_string(),
                "Task 2".to_string(),
                "Queued".to_string(),
                vec!["task1".to_string()],
            ),
            (
                "task3".to_string(),
                "Task 3".to_string(),
                "Running".to_string(),
                vec![],
            ),
            (
                "task4".to_string(),
                "Task 4".to_string(),
                "Queued".to_string(),
                vec!["task3".to_string()],
            ),
        ];

        manager.build_graph(&tasks);

        let graph = manager.get_graph().unwrap();
        assert_eq!(graph.stats.connected_components, 2);
    }
}
