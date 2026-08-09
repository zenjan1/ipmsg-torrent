//! Bandwidth Allocation Strategies (Phase 96)
//!
//! Support multiple bandwidth allocation strategies for intelligent distribution
//! of available bandwidth among download tasks.

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Allocation strategy types
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AllocationStrategy {
    /// Fair allocation: equal bandwidth for all active tasks
    #[default]
    Fair,
    /// Priority-based allocation: higher priority gets more bandwidth
    Priority,
    /// Proportional allocation: based on bandwidth_weight field
    Proportional,
}

impl std::fmt::Display for AllocationStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fair => write!(f, "fair"),
            Self::Priority => write!(f, "priority"),
            Self::Proportional => write!(f, "proportional"),
        }
    }
}

/// Configuration for bandwidth allocation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationConfig {
    /// Enable bandwidth allocation
    pub enabled: bool,
    /// Allocation strategy to use
    pub strategy: AllocationStrategy,
    /// Minimum bandwidth guarantee per task (bytes/sec)
    pub min_bandwidth_bps: u64,
    /// Maximum bandwidth limit per task (bytes/sec, 0 = unlimited)
    pub max_bandwidth_bps: u64,
    /// Recalculation interval in seconds
    pub recalc_interval_secs: u64,
}

impl Default for AllocationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            strategy: AllocationStrategy::Fair,
            min_bandwidth_bps: 10 * 1024, // 10 KB/s
            max_bandwidth_bps: 0,         // unlimited
            recalc_interval_secs: 5,
        }
    }
}

/// Task data needed for allocation calculation
#[derive(Debug, Clone)]
pub struct TaskAllocationData {
    pub task_id: String,
    pub priority: i32,
    pub bandwidth_weight: u8,
    pub is_active: bool,
}

/// Allocation plan for a single task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAllocation {
    pub task_id: String,
    pub allocated_bps: u64,
    pub strategy: AllocationStrategy,
    pub weight: f64,
}

/// Complete allocation plan for all tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationPlan {
    pub strategy: AllocationStrategy,
    pub total_bandwidth_bps: u64,
    pub allocated_bandwidth_bps: u64,
    pub task_allocations: Vec<TaskAllocation>,
    pub calculated_at: u64,
}

/// Manager for bandwidth allocation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllocationManager {
    config: AllocationConfig,
    last_calculation: Option<AllocationPlan>,
    last_calculated_at: u64,
}

impl Default for AllocationManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AllocationManager {
    /// Create a new manager with default config
    pub fn new() -> Self {
        Self {
            config: AllocationConfig::default(),
            last_calculation: None,
            last_calculated_at: 0,
        }
    }

    /// Create with custom config
    pub fn with_config(config: AllocationConfig) -> Self {
        Self {
            config,
            last_calculation: None,
            last_calculated_at: 0,
        }
    }

    /// Get current config
    pub fn config(&self) -> &AllocationConfig {
        &self.config
    }

    /// Update config
    pub fn set_config(&mut self, config: AllocationConfig) {
        self.config = config;
    }

    /// Get last allocation plan
    pub fn last_plan(&self) -> Option<&AllocationPlan> {
        self.last_calculation.as_ref()
    }

    /// Check if recalculation is needed
    pub fn needs_recalculation(&self) -> bool {
        if !self.config.enabled {
            return false;
        }
        let now = current_epoch_secs();
        now - self.last_calculated_at >= self.config.recalc_interval_secs
    }

    /// Calculate allocation plan for given tasks and total bandwidth
    pub fn calculate_allocation(
        &mut self,
        total_bandwidth_bps: u64,
        tasks: &[TaskAllocationData],
    ) -> AllocationPlan {
        let active_tasks: Vec<&TaskAllocationData> = tasks.iter().filter(|t| t.is_active).collect();

        if active_tasks.is_empty() {
            let plan = AllocationPlan {
                strategy: self.config.strategy,
                total_bandwidth_bps,
                allocated_bandwidth_bps: 0,
                task_allocations: Vec::new(),
                calculated_at: current_epoch_secs(),
            };
            self.last_calculation = Some(plan.clone());
            self.last_calculated_at = plan.calculated_at;
            return plan;
        }

        let task_allocations = match self.config.strategy {
            AllocationStrategy::Fair => {
                self.calculate_fair_allocation(total_bandwidth_bps, &active_tasks)
            }
            AllocationStrategy::Priority => {
                self.calculate_priority_allocation(total_bandwidth_bps, &active_tasks)
            }
            AllocationStrategy::Proportional => {
                self.calculate_proportional_allocation(total_bandwidth_bps, &active_tasks)
            }
        };

        let allocated: u64 = task_allocations.iter().map(|a| a.allocated_bps).sum();

        let plan = AllocationPlan {
            strategy: self.config.strategy,
            total_bandwidth_bps,
            allocated_bandwidth_bps: allocated,
            task_allocations,
            calculated_at: current_epoch_secs(),
        };

        self.last_calculation = Some(plan.clone());
        self.last_calculated_at = plan.calculated_at;
        plan
    }

    /// Fair allocation: equal share for all active tasks
    fn calculate_fair_allocation(
        &self,
        total_bandwidth_bps: u64,
        tasks: &[&TaskAllocationData],
    ) -> Vec<TaskAllocation> {
        let count = tasks.len() as u64;
        if count == 0 {
            return Vec::new();
        }

        let base_share = total_bandwidth_bps / count;

        tasks
            .iter()
            .map(|task| {
                let mut allocated = base_share;

                // Apply minimum guarantee
                if allocated < self.config.min_bandwidth_bps {
                    allocated = self.config.min_bandwidth_bps;
                }

                // Apply maximum limit
                if self.config.max_bandwidth_bps > 0 && allocated > self.config.max_bandwidth_bps {
                    allocated = self.config.max_bandwidth_bps;
                }

                TaskAllocation {
                    task_id: task.task_id.clone(),
                    allocated_bps: allocated,
                    strategy: AllocationStrategy::Fair,
                    weight: 1.0 / count as f64,
                }
            })
            .collect()
    }

    /// Priority allocation: higher priority gets more bandwidth
    fn calculate_priority_allocation(
        &self,
        total_bandwidth_bps: u64,
        tasks: &[&TaskAllocationData],
    ) -> Vec<TaskAllocation> {
        if tasks.is_empty() {
            return Vec::new();
        }

        // Priority weights: High(3)=4x, Normal(2)=2x, Low(1)=1x
        let priority_weight = |priority: i32| -> f64 {
            match priority {
                p if p >= 3 => 4.0,
                p if p >= 2 => 2.0,
                _ => 1.0,
            }
        };

        let total_weight: f64 = tasks.iter().map(|t| priority_weight(t.priority)).sum();

        if total_weight == 0.0 {
            return self.calculate_fair_allocation(total_bandwidth_bps, tasks);
        }

        tasks
            .iter()
            .map(|task| {
                let weight = priority_weight(task.priority);
                let proportion = weight / total_weight;
                let mut allocated = (total_bandwidth_bps as f64 * proportion) as u64;

                // Apply minimum guarantee
                if allocated < self.config.min_bandwidth_bps {
                    allocated = self.config.min_bandwidth_bps;
                }

                // Apply maximum limit
                if self.config.max_bandwidth_bps > 0 && allocated > self.config.max_bandwidth_bps {
                    allocated = self.config.max_bandwidth_bps;
                }

                TaskAllocation {
                    task_id: task.task_id.clone(),
                    allocated_bps: allocated,
                    strategy: AllocationStrategy::Priority,
                    weight: proportion,
                }
            })
            .collect()
    }

    /// Proportional allocation: based on bandwidth_weight field
    fn calculate_proportional_allocation(
        &self,
        total_bandwidth_bps: u64,
        tasks: &[&TaskAllocationData],
    ) -> Vec<TaskAllocation> {
        if tasks.is_empty() {
            return Vec::new();
        }

        let total_weight: u64 = tasks.iter().map(|t| t.bandwidth_weight as u64).sum();

        if total_weight == 0 {
            return self.calculate_fair_allocation(total_bandwidth_bps, tasks);
        }

        tasks
            .iter()
            .map(|task| {
                let weight = task.bandwidth_weight as u64;
                let proportion = weight as f64 / total_weight as f64;
                let mut allocated = (total_bandwidth_bps as f64 * proportion) as u64;

                // Apply minimum guarantee
                if allocated < self.config.min_bandwidth_bps {
                    allocated = self.config.min_bandwidth_bps;
                }

                // Apply maximum limit
                if self.config.max_bandwidth_bps > 0 && allocated > self.config.max_bandwidth_bps {
                    allocated = self.config.max_bandwidth_bps;
                }

                TaskAllocation {
                    task_id: task.task_id.clone(),
                    allocated_bps: allocated,
                    strategy: AllocationStrategy::Proportional,
                    weight: proportion,
                }
            })
            .collect()
    }

    /// Get allocation for a specific task from last plan
    pub fn get_task_allocation(&self, task_id: &str) -> Option<&TaskAllocation> {
        self.last_calculation
            .as_ref()?
            .task_allocations
            .iter()
            .find(|a| a.task_id == task_id)
    }
}

fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Persistence error type
#[derive(Debug)]
pub enum AllocationPersistenceError {
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl std::fmt::Display for AllocationPersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Json(e) => write!(f, "JSON error: {}", e),
        }
    }
}

impl From<std::io::Error> for AllocationPersistenceError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for AllocationPersistenceError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

/// Save allocation config to disk (atomic write)
pub fn save_allocation_config(
    config: &AllocationConfig,
    data_dir: &std::path::Path,
) -> Result<(), AllocationPersistenceError> {
    let path = data_dir.join("allocation_config.json");
    let json = serde_json::to_string_pretty(config)?;
    let tmp_path = data_dir.join("allocation_config.json.tmp");
    std::fs::write(&tmp_path, json.as_bytes())?;
    std::fs::rename(&tmp_path, &path)?;
    Ok(())
}

/// Load allocation config from disk
pub fn load_allocation_config(
    data_dir: &std::path::Path,
) -> Result<Option<AllocationConfig>, AllocationPersistenceError> {
    let path = data_dir.join("allocation_config.json");
    if !path.exists() {
        return Ok(None);
    }
    let data = std::fs::read_to_string(&path)?;
    let config: AllocationConfig = serde_json::from_str(&data)?;
    Ok(Some(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_task(id: &str, priority: i32, weight: u8, active: bool) -> TaskAllocationData {
        TaskAllocationData {
            task_id: id.to_string(),
            priority,
            bandwidth_weight: weight,
            is_active: active,
        }
    }

    #[test]
    fn test_strategy_default() {
        assert_eq!(AllocationStrategy::default(), AllocationStrategy::Fair);
    }

    #[test]
    fn test_strategy_display() {
        assert_eq!(AllocationStrategy::Fair.to_string(), "fair");
        assert_eq!(AllocationStrategy::Priority.to_string(), "priority");
        assert_eq!(AllocationStrategy::Proportional.to_string(), "proportional");
    }

    #[test]
    fn test_config_default() {
        let config = AllocationConfig::default();
        assert!(config.enabled);
        assert_eq!(config.strategy, AllocationStrategy::Fair);
        assert_eq!(config.min_bandwidth_bps, 10 * 1024);
        assert_eq!(config.max_bandwidth_bps, 0);
        assert_eq!(config.recalc_interval_secs, 5);
    }

    #[test]
    fn test_manager_new() {
        let mgr = AllocationManager::new();
        assert!(mgr.config().enabled);
        assert!(mgr.last_plan().is_none());
    }

    #[test]
    fn test_fair_allocation_equal_share() {
        let mut mgr = AllocationManager::new();
        mgr.set_config(AllocationConfig {
            min_bandwidth_bps: 0,
            ..Default::default()
        });
        let tasks = vec![
            make_task("t1", 2, 1, true),
            make_task("t2", 2, 1, true),
            make_task("t3", 2, 1, true),
        ];

        let plan = mgr.calculate_allocation(3000, &tasks);

        assert_eq!(plan.strategy, AllocationStrategy::Fair);
        assert_eq!(plan.total_bandwidth_bps, 3000);
        assert_eq!(plan.task_allocations.len(), 3);

        // Each task should get 1000 bps
        for alloc in &plan.task_allocations {
            assert_eq!(alloc.allocated_bps, 1000);
            assert!((alloc.weight - 1.0 / 3.0).abs() < 0.01);
        }
    }

    #[test]
    fn test_fair_allocation_respects_min_bandwidth() {
        let mut mgr = AllocationManager::with_config(AllocationConfig {
            min_bandwidth_bps: 500,
            ..Default::default()
        });

        let tasks = vec![make_task("t1", 2, 1, true), make_task("t2", 2, 1, true)];

        // Total 600, fair share would be 300 each, but min is 500
        let plan = mgr.calculate_allocation(600, &tasks);

        for alloc in &plan.task_allocations {
            assert_eq!(alloc.allocated_bps, 500);
        }
    }

    #[test]
    fn test_fair_allocation_respects_max_bandwidth() {
        let mut mgr = AllocationManager::with_config(AllocationConfig {
            max_bandwidth_bps: 800,
            ..Default::default()
        });

        let tasks = vec![make_task("t1", 2, 1, true), make_task("t2", 2, 1, true)];

        // Total 2000, fair share would be 1000 each, but max is 800
        let plan = mgr.calculate_allocation(2000, &tasks);

        for alloc in &plan.task_allocations {
            assert_eq!(alloc.allocated_bps, 800);
        }
    }

    #[test]
    fn test_priority_allocation_high_priority_gets_more() {
        let mut mgr = AllocationManager::new();
        mgr.set_config(AllocationConfig {
            strategy: AllocationStrategy::Priority,
            min_bandwidth_bps: 0,
            ..Default::default()
        });

        let tasks = vec![
            make_task("t1", 3, 1, true), // High priority (4x weight)
            make_task("t2", 1, 1, true), // Low priority (1x weight)
        ];

        let plan = mgr.calculate_allocation(5000, &tasks);

        assert_eq!(plan.strategy, AllocationStrategy::Priority);

        let high = plan
            .task_allocations
            .iter()
            .find(|a| a.task_id == "t1")
            .unwrap();
        let low = plan
            .task_allocations
            .iter()
            .find(|a| a.task_id == "t2")
            .unwrap();

        // High should get 4x more than low (4:1 ratio)
        assert!(high.allocated_bps > low.allocated_bps);
        assert!((high.weight - 0.8).abs() < 0.01); // 4/5 = 0.8
        assert!((low.weight - 0.2).abs() < 0.01); // 1/5 = 0.2
    }

    #[test]
    fn test_priority_allocation_three_levels() {
        let mut mgr = AllocationManager::new();
        mgr.set_config(AllocationConfig {
            strategy: AllocationStrategy::Priority,
            min_bandwidth_bps: 0,
            ..Default::default()
        });

        let tasks = vec![
            make_task("t1", 3, 1, true), // High (4x)
            make_task("t2", 2, 1, true), // Normal (2x)
            make_task("t3", 1, 1, true), // Low (1x)
        ];

        let plan = mgr.calculate_allocation(7000, &tasks);

        let high = plan
            .task_allocations
            .iter()
            .find(|a| a.task_id == "t1")
            .unwrap();
        let normal = plan
            .task_allocations
            .iter()
            .find(|a| a.task_id == "t2")
            .unwrap();
        let low = plan
            .task_allocations
            .iter()
            .find(|a| a.task_id == "t3")
            .unwrap();

        // Total weight = 4+2+1 = 7
        assert!((high.weight - 4.0 / 7.0).abs() < 0.01);
        assert!((normal.weight - 2.0 / 7.0).abs() < 0.01);
        assert!((low.weight - 1.0 / 7.0).abs() < 0.01);
    }

    #[test]
    fn test_proportional_allocation_by_weight() {
        let mut mgr = AllocationManager::new();
        mgr.set_config(AllocationConfig {
            strategy: AllocationStrategy::Proportional,
            min_bandwidth_bps: 0,
            ..Default::default()
        });

        let tasks = vec![
            make_task("t1", 2, 3, true), // weight 3
            make_task("t2", 2, 1, true), // weight 1
        ];

        let plan = mgr.calculate_allocation(4000, &tasks);

        assert_eq!(plan.strategy, AllocationStrategy::Proportional);

        let t1 = plan
            .task_allocations
            .iter()
            .find(|a| a.task_id == "t1")
            .unwrap();
        let t2 = plan
            .task_allocations
            .iter()
            .find(|a| a.task_id == "t2")
            .unwrap();

        // Total weight = 3+1 = 4
        // t1 gets 3/4 = 3000, t2 gets 1/4 = 1000
        assert_eq!(t1.allocated_bps, 3000);
        assert_eq!(t2.allocated_bps, 1000);
        assert!((t1.weight - 0.75).abs() < 0.01);
        assert!((t2.weight - 0.25).abs() < 0.01);
    }

    #[test]
    fn test_proportional_allocation_zero_weight_fallback() {
        let mut mgr = AllocationManager::new();
        mgr.set_config(AllocationConfig {
            strategy: AllocationStrategy::Proportional,
            min_bandwidth_bps: 0,
            ..Default::default()
        });

        let tasks = vec![make_task("t1", 2, 0, true), make_task("t2", 2, 0, true)];

        let plan = mgr.calculate_allocation(2000, &tasks);

        // Should fall back to fair allocation
        for alloc in &plan.task_allocations {
            assert_eq!(alloc.allocated_bps, 1000);
        }
    }

    #[test]
    fn test_only_active_tasks_get_allocation() {
        let mut mgr = AllocationManager::new();
        mgr.set_config(AllocationConfig {
            min_bandwidth_bps: 0,
            ..Default::default()
        });

        let tasks = vec![
            make_task("t1", 2, 1, true),
            make_task("t2", 2, 1, false), // inactive
            make_task("t3", 2, 1, true),
        ];

        let plan = mgr.calculate_allocation(2000, &tasks);

        assert_eq!(plan.task_allocations.len(), 2);
        assert!(plan.task_allocations.iter().all(|a| a.task_id != "t2"));

        // Each active task gets 1000
        for alloc in &plan.task_allocations {
            assert_eq!(alloc.allocated_bps, 1000);
        }
    }

    #[test]
    fn test_no_active_tasks() {
        let mut mgr = AllocationManager::new();

        let tasks = vec![make_task("t1", 2, 1, false), make_task("t2", 2, 1, false)];

        let plan = mgr.calculate_allocation(2000, &tasks);

        assert_eq!(plan.task_allocations.len(), 0);
        assert_eq!(plan.allocated_bandwidth_bps, 0);
    }

    #[test]
    fn test_last_plan_stored() {
        let mut mgr = AllocationManager::new();

        assert!(mgr.last_plan().is_none());

        let tasks = vec![make_task("t1", 2, 1, true)];
        mgr.calculate_allocation(1000, &tasks);

        assert!(mgr.last_plan().is_some());
        let plan = mgr.last_plan().unwrap();
        assert_eq!(plan.task_allocations.len(), 1);
    }

    #[test]
    fn test_get_task_allocation() {
        let mut mgr = AllocationManager::new();
        mgr.set_config(AllocationConfig {
            min_bandwidth_bps: 0,
            ..Default::default()
        });

        let tasks = vec![make_task("t1", 2, 1, true), make_task("t2", 2, 1, true)];

        mgr.calculate_allocation(2000, &tasks);

        let alloc = mgr.get_task_allocation("t1");
        assert!(alloc.is_some());
        assert_eq!(alloc.unwrap().allocated_bps, 1000);

        let missing = mgr.get_task_allocation("t999");
        assert!(missing.is_none());
    }

    #[test]
    fn test_needs_recalculation() {
        let mut mgr = AllocationManager::new();

        // Initially needs calculation
        assert!(mgr.needs_recalculation());

        let tasks = vec![make_task("t1", 2, 1, true)];
        mgr.calculate_allocation(1000, &tasks);

        // Just calculated, shouldn't need recalculation
        assert!(!mgr.needs_recalculation());
    }

    #[test]
    fn test_needs_recalculation_disabled() {
        let mut mgr = AllocationManager::with_config(AllocationConfig {
            enabled: false,
            ..Default::default()
        });

        assert!(!mgr.needs_recalculation());
    }

    #[test]
    fn test_config_serialization() {
        let config = AllocationConfig {
            enabled: true,
            strategy: AllocationStrategy::Priority,
            min_bandwidth_bps: 5000,
            max_bandwidth_bps: 100000,
            recalc_interval_secs: 10,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: AllocationConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.enabled, config.enabled);
        assert_eq!(deserialized.strategy, config.strategy);
        assert_eq!(deserialized.min_bandwidth_bps, config.min_bandwidth_bps);
        assert_eq!(deserialized.max_bandwidth_bps, config.max_bandwidth_bps);
        assert_eq!(
            deserialized.recalc_interval_secs,
            config.recalc_interval_secs
        );
    }

    #[test]
    fn test_plan_serialization() {
        let mut mgr = AllocationManager::new();
        let tasks = vec![make_task("t1", 2, 1, true), make_task("t2", 3, 2, true)];

        let plan = mgr.calculate_allocation(3000, &tasks);
        let json = serde_json::to_string(&plan).unwrap();
        let deserialized: AllocationPlan = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.strategy, plan.strategy);
        assert_eq!(deserialized.total_bandwidth_bps, plan.total_bandwidth_bps);
        assert_eq!(
            deserialized.task_allocations.len(),
            plan.task_allocations.len()
        );
    }

    #[test]
    fn test_config_save_load() {
        let config = AllocationConfig::default();
        let dir = std::env::temp_dir().join("test_allocation_config");
        let _ = std::fs::create_dir_all(&dir);

        save_allocation_config(&config, &dir).unwrap();
        let loaded = load_allocation_config(&dir).unwrap().unwrap();

        assert_eq!(loaded.strategy, config.strategy);
        assert_eq!(loaded.min_bandwidth_bps, config.min_bandwidth_bps);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_config_load_missing() {
        let dir = std::env::temp_dir().join("test_allocation_nonexistent");
        let result = load_allocation_config(&dir).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_manager_set_config() {
        let mut mgr = AllocationManager::new();
        assert_eq!(mgr.config().strategy, AllocationStrategy::Fair);

        mgr.set_config(AllocationConfig {
            strategy: AllocationStrategy::Proportional,
            ..Default::default()
        });

        assert_eq!(mgr.config().strategy, AllocationStrategy::Proportional);
    }

    #[test]
    fn test_min_and_max_both_applied() {
        let mut mgr = AllocationManager::with_config(AllocationConfig {
            min_bandwidth_bps: 1000,
            max_bandwidth_bps: 2000,
            ..Default::default()
        });

        let tasks = vec![make_task("t1", 2, 1, true)];

        // Total 500, would be 500 for t1, but min is 1000
        let plan = mgr.calculate_allocation(500, &tasks);
        assert_eq!(plan.task_allocations[0].allocated_bps, 1000);

        // Total 10000, would be 10000 for t1, but max is 2000
        let plan = mgr.calculate_allocation(10000, &tasks);
        assert_eq!(plan.task_allocations[0].allocated_bps, 2000);
    }
}
