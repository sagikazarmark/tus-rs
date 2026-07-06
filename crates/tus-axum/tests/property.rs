//! Property-style integration tests for core protocol invariants.

use axum::{
    Router,
    body::Body,
    http::{Method, Request, Response, StatusCode},
};
use http_body_util::BodyExt;
use proptest::{
    collection::vec,
    prelude::*,
    test_runner::{Config as ProptestConfig, TestRunner},
};
use tower::ServiceExt;
use tus_axum::{RouterOptions, TusState, create_router};
use tus_protocol::{
    Config, NoopHookExecutor, ProtocolHandle, TUS_RESUMABLE, locking::memory::MemoryLocker,
    state::memory::MemoryStateStore, storage::memory::MemoryStorage,
};

fn build_router() -> Router {
    let config = Config::all_extensions().with_base_path("/files");
    let state = TusState::new(ProtocolHandle::new(
        config,
        MemoryStorage::new(),
        MemoryStateStore::new(),
        MemoryLocker::new(),
        NoopHookExecutor::new(),
    ));
    create_router(state, RouterOptions::default()).unwrap()
}

fn tus_request(method: Method, uri: &str) -> axum::http::request::Builder {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("tus-resumable", TUS_RESUMABLE)
}

async fn send(router: Router, req: Request<Body>) -> Response<Body> {
    router.oneshot(req).await.expect("router must not fail")
}

fn upload_id_from_location(location: &str) -> &str {
    location.rsplit('/').next().expect("location has an id")
}

async fn response_text(response: Response<Body>) -> String {
    String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .expect("body collectable")
            .to_bytes()
            .to_vec(),
    )
    .expect("response body must be utf-8")
}

async fn create_partial(router: &Router, body: &[u8]) -> String {
    let post = send(
        router.clone(),
        tus_request(Method::POST, "/files")
            .header("upload-length", body.len())
            .header("upload-concat", "partial")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(post.status(), StatusCode::CREATED);

    let id = upload_id_from_location(post.headers().get("location").unwrap().to_str().unwrap())
        .to_string();
    let item = format!("/files/{id}");

    let patch = send(
        router.clone(),
        tus_request(Method::PATCH, &item)
            .header("content-type", "application/offset+octet-stream")
            .header("upload-offset", "0")
            .header("content-length", body.len())
            .body(Body::from(body.to_vec()))
            .unwrap(),
    )
    .await;
    assert_eq!(patch.status(), StatusCode::NO_CONTENT);

    id
}

#[test]
fn concatenation_length_matches_sum_of_parts() {
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 24,
        ..ProptestConfig::default()
    });

    runner
        .run(&vec(vec(any::<u8>(), 1..8), 1..5), |parts| {
            tokio_test::block_on(async {
                let router = build_router();
                let mut part_ids = Vec::new();
                let total_len: usize = parts.iter().map(Vec::len).sum();

                for part in &parts {
                    part_ids.push(create_partial(&router, part).await);
                }

                let concat_value = format!(
                    "final;{}",
                    part_ids
                        .iter()
                        .map(|id| format!("/files/{id}"))
                        .collect::<Vec<_>>()
                        .join(" ")
                );
                let response = send(
                    router.clone(),
                    tus_request(Method::POST, "/files")
                        .header("upload-concat", &concat_value)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await;
                prop_assert_eq!(response.status(), StatusCode::CREATED);
                prop_assert_eq!(
                    response.headers().get("upload-length").unwrap(),
                    &total_len.to_string()
                );
                prop_assert_eq!(
                    response.headers().get("upload-offset").unwrap(),
                    &total_len.to_string()
                );

                let final_item = format!(
                    "/files/{}",
                    upload_id_from_location(
                        response
                            .headers()
                            .get("location")
                            .unwrap()
                            .to_str()
                            .unwrap()
                    )
                );
                let head = send(
                    router,
                    tus_request(Method::HEAD, &final_item)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await;
                let upload_concat = head
                    .headers()
                    .get("upload-concat")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();
                prop_assert_eq!(
                    head.headers().get("upload-length").unwrap(),
                    &total_len.to_string()
                );
                prop_assert_eq!(
                    head.headers().get("upload-offset").unwrap(),
                    &total_len.to_string()
                );
                for id in &part_ids {
                    let needle = format!("/files/{id}");
                    prop_assert!(upload_concat.contains(&needle));
                }

                Ok(())
            })
        })
        .unwrap();
}

#[test]
fn deleting_a_partial_rejects_future_concatenation() {
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 16,
        ..ProptestConfig::default()
    });

    runner
        .run(
            &(vec(any::<u8>(), 1..8), vec(any::<u8>(), 1..8)),
            |(first, second)| {
                tokio_test::block_on(async {
                    let router = build_router();
                    let first_id = create_partial(&router, &first).await;
                    let second_id = create_partial(&router, &second).await;

                    let delete = send(
                        router.clone(),
                        tus_request(Method::DELETE, &format!("/files/{second_id}"))
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await;
                    prop_assert_eq!(delete.status(), StatusCode::NO_CONTENT);

                    let concat_value = format!("final;/files/{first_id} /files/{second_id}");
                    let response = send(
                        router,
                        tus_request(Method::POST, "/files")
                            .header("upload-concat", concat_value)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await;
                    let body = response_text(response).await;
                    prop_assert!(body.contains(&second_id));

                    Ok(())
                })
            },
        )
        .unwrap();
}

#[test]
fn concurrent_patch_attempts_keep_offset_monotonic() {
    let mut runner = TestRunner::new(ProptestConfig {
        cases: 24,
        ..ProptestConfig::default()
    });

    runner
        .run(
            &(vec(any::<u8>(), 1..5), vec(any::<u8>(), 1..5)),
            |(left, right)| {
                tokio_test::block_on(async {
                    let router = build_router();
                    let length = left.len().max(right.len());

                    let post = send(
                        router.clone(),
                        tus_request(Method::POST, "/files")
                            .header("upload-length", length)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await;
                    prop_assert_eq!(post.status(), StatusCode::CREATED);
                    let item = format!(
                        "/files/{}",
                        upload_id_from_location(
                            post.headers().get("location").unwrap().to_str().unwrap()
                        )
                    );

                    let left_request = tus_request(Method::PATCH, &item)
                        .header("content-type", "application/offset+octet-stream")
                        .header("upload-offset", "0")
                        .header("content-length", left.len())
                        .body(Body::from(left.clone()))
                        .unwrap();
                    let right_request = tus_request(Method::PATCH, &item)
                        .header("content-type", "application/offset+octet-stream")
                        .header("upload-offset", "0")
                        .header("content-length", right.len())
                        .body(Body::from(right.clone()))
                        .unwrap();

                    let (left_response, right_response) = tokio::join!(
                        send(router.clone(), left_request),
                        send(router.clone(), right_request)
                    );

                    let successful_bytes = [
                        (left_response.status() == StatusCode::NO_CONTENT).then_some(left.len()),
                        (right_response.status() == StatusCode::NO_CONTENT).then_some(right.len()),
                    ]
                    .into_iter()
                    .flatten()
                    .sum::<usize>();

                    prop_assert!(
                        successful_bytes == 0
                            || successful_bytes == left.len()
                            || successful_bytes == right.len()
                    );

                    let head = send(
                        router,
                        tus_request(Method::HEAD, &item)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await;
                    let offset = head
                        .headers()
                        .get("upload-offset")
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .parse::<usize>()
                        .unwrap();
                    prop_assert_eq!(offset, successful_bytes);
                    prop_assert!(offset <= length);

                    Ok(())
                })
            },
        )
        .unwrap();
}
