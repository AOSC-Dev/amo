use oma_pm::apt::OmaOperation;
use oma_tum::TopicUpdateEntryRef;
use serde::Serialize;
use std::collections::BTreeMap;
use tracing::warn;

#[derive(Debug, Serialize)]
pub(crate) struct UpdatesListResponse {
    #[serde(flatten)]
    operation: OmaOperation,
    has_important_updates: bool,
    tum: Vec<TumEntry>,
}

#[derive(Debug, Serialize)]
pub(crate) struct TumEntry {
    id: String,
    kind: &'static str,
    security: bool,
    name: BTreeMap<String, String>,
    caution: Option<BTreeMap<String, String>>,
    packages: Vec<String>,
    topics: Vec<String>,
    package_count: usize,
}

fn tum_entry(id: &str, entry: TopicUpdateEntryRef<'_>) -> TumEntry {
    match &entry {
        TopicUpdateEntryRef::Conventional {
            security,
            packages,
            packages_v2,
            name,
            caution,
        } => {
            let mut package_names = if packages_v2.is_empty() {
                packages.keys().cloned().collect::<Vec<_>>()
            } else {
                packages_v2.keys().cloned().collect::<Vec<_>>()
            };
            package_names.sort_unstable();

            TumEntry {
                id: id.to_string(),
                kind: "conventional",
                security: *security,
                name: name.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                caution: caution
                    .as_ref()
                    .map(|values| values.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
                package_count: entry.count_packages(),
                packages: package_names,
                topics: Vec::new(),
            }
        }
        TopicUpdateEntryRef::Cumulative {
            security,
            name,
            caution,
            topics,
            ..
        } => TumEntry {
            id: id.to_string(),
            kind: "cumulative",
            security: *security,
            name: name.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            caution: caution
                .as_ref()
                .map(|values| values.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
            packages: Vec::new(),
            topics: topics.to_vec(),
            package_count: entry.count_packages(),
        },
    }
}

pub(crate) fn updates_list_response(
    lists_dir: &str,
    operation: OmaOperation,
) -> UpdatesListResponse {
    let mut tum = match oma_tum::get_tum(lists_dir) {
        Ok(manifests) => oma_tum::get_matches_tum(&manifests, &operation)
            .into_iter()
            .map(|(id, entry)| tum_entry(id, entry))
            .collect::<Vec<_>>(),
        Err(e) => {
            warn!(
                error = e.to_string(),
                "Failed to load topic update manifests"
            );
            Vec::new()
        }
    };

    tum.sort_unstable_by(|a, b| b.security.cmp(&a.security).then_with(|| a.id.cmp(&b.id)));
    let has_important_updates = tum.iter().any(|entry| entry.security);

    UpdatesListResponse {
        operation,
        has_important_updates,
        tum,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oma_pm::apt::{InstallEntry, InstallOperation, PackageUrl};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TMP_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn upgrade_entry(name: &str, old_version: &str, new_version: &str) -> InstallEntry {
        InstallEntry::builder()
            .name(name.to_string())
            .name_without_arch(name.to_string())
            .old_version(old_version.to_string())
            .new_version(new_version.to_string())
            .new_size(1)
            .pkg_urls(vec![PackageUrl {
                download_url: String::new(),
                index_url: String::new(),
            }])
            .arch("amd64".to_string())
            .download_size(1)
            .op(InstallOperation::Upgrade)
            .index(0)
            .build()
    }

    fn operation_with(packages: &[&str]) -> OmaOperation {
        OmaOperation {
            install: packages
                .iter()
                .map(|name| upgrade_entry(name, "1.0", "2.0"))
                .collect(),
            remove: Vec::new(),
            disk_size_delta: 0,
            autoremovable: (0, 0),
            total_download_size: 1,
            suggest: Vec::new(),
            recommend: Vec::new(),
        }
    }

    fn matched_topic(security: bool) -> TumEntry {
        let json = format!(
            r#"{{
                "test-topic": {{
                    "type": "conventional",
                    "security": {security},
                    "name": {{"default": "Test topic"}},
                    "caution": {{"default": "Test caution"}},
                    "packages": {{"test-package": "2.0"}}
                }}
            }}"#
        );
        let manifest = oma_tum::parse_single_tum(json.as_bytes()).unwrap();
        let manifests = vec![manifest];
        let operation = operation_with(&["test-package"]);
        let mut matches = oma_tum::get_matches_tum(&manifests, &operation);
        tum_entry("test-topic", matches.remove("test-topic").unwrap())
    }

    fn temp_tum_dir() -> PathBuf {
        let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("amo-tum-test-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_manifest(dir: &Path, name: &str, json: &str) {
        std::fs::write(dir.join(name), json).unwrap();
    }

    #[test]
    fn conventional_entry_carries_security_flag() {
        let topic = matched_topic(true);
        assert!(topic.security);
        assert_eq!(topic.kind, "conventional");
        assert_eq!(topic.packages, vec!["test-package".to_string()]);
        assert_eq!(topic.package_count, 1);
    }

    #[test]
    fn non_security_conventional_entry_is_not_security() {
        let topic = matched_topic(false);
        assert!(!topic.security);
    }

    #[test]
    fn v2_packages_are_preferred_for_names_and_count() {
        let json = r#"{
            "test-topic": {
                "type": "conventional",
                "security": true,
                "name": {"default": "Test topic"},
                "packages": {"old-package": "2.0"},
                "packages-v2": {"new-package": ">= 2.0"}
            }
        }"#;
        let manifest = oma_tum::parse_single_tum(json.as_bytes()).unwrap();
        let manifests = vec![manifest];
        let operation = OmaOperation {
            install: vec![upgrade_entry("new-package", "2.0", "2.5")],
            remove: Vec::new(),
            disk_size_delta: 0,
            autoremovable: (0, 0),
            total_download_size: 1,
            suggest: Vec::new(),
            recommend: Vec::new(),
        };
        let mut matches = oma_tum::get_matches_tum(&manifests, &operation);
        let topic = tum_entry("test-topic", matches.remove("test-topic").unwrap());
        assert_eq!(topic.packages, vec!["new-package".to_string()]);
        assert_eq!(topic.package_count, 1);
    }

    #[test]
    fn cumulative_entry_aggregates_matched_topics() {
        let json = r#"{
            "topic-a": {
                "type": "conventional",
                "security": false,
                "name": {"default": "Topic A"},
                "packages": {"test-package": "2.0"}
            },
            "topic-b": {
                "type": "conventional",
                "security": false,
                "name": {"default": "Topic B"},
                "packages": {"other-package": "2.0"}
            },
            "cumulative-topic": {
                "type": "cumulative",
                "security": true,
                "name": {"default": "Cumulative topic"},
                "topics": ["topic-a", "topic-b"]
            }
        }"#;
        let manifest = oma_tum::parse_single_tum(json.as_bytes()).unwrap();
        let manifests = vec![manifest];
        let operation = operation_with(&["test-package", "other-package"]);
        let mut matches = oma_tum::get_matches_tum(&manifests, &operation);
        let cumulative = tum_entry(
            "cumulative-topic",
            matches.remove("cumulative-topic").unwrap(),
        );
        assert_eq!(cumulative.kind, "cumulative");
        assert!(cumulative.security);
        assert_eq!(
            cumulative.topics,
            vec!["topic-a".to_string(), "topic-b".to_string()]
        );
        assert_eq!(cumulative.package_count, 2);
        assert!(cumulative.packages.is_empty());
    }

    #[test]
    fn response_sorts_security_first_and_aggregates_important() {
        let dir = temp_tum_dir();
        write_manifest(
            &dir,
            "a-updates.json",
            r#"{
                "non-security": {
                    "type": "conventional",
                    "security": false,
                    "name": {"default": "Non-security"},
                    "packages": {"test-package": "2.0"}
                }
            }"#,
        );
        write_manifest(
            &dir,
            "b-updates.json",
            r#"{
                "security-topic": {
                    "type": "conventional",
                    "security": true,
                    "name": {"default": "Security"},
                    "packages": {"other-package": "2.0"}
                }
            }"#,
        );
        let operation = operation_with(&["test-package", "other-package"]);
        let response = updates_list_response(dir.to_str().unwrap(), operation);
        assert!(response.has_important_updates);
        let ids = response
            .tum
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["security-topic", "non-security"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn response_has_important_is_false_without_security() {
        let dir = temp_tum_dir();
        write_manifest(
            &dir,
            "a-updates.json",
            r#"{
                "non-security": {
                    "type": "conventional",
                    "security": false,
                    "name": {"default": "Non-security"},
                    "packages": {"test-package": "2.0"}
                }
            }"#,
        );
        let operation = operation_with(&["test-package"]);
        let response = updates_list_response(dir.to_str().unwrap(), operation);
        assert!(!response.has_important_updates);
        assert_eq!(response.tum.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
