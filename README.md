# 수어 칠판 · KSL Board

### 수어 학습 앱을 위한 실시간 로그 통합 파이프라인

카메라로 수어 단어를 읽고, 본보기 동작을 따라 연습하며, 학습 기록을 AWS에 수집하는 모바일 팀 프로젝트입니다. iOS와 Android에서 동작 좌표를 추출하고 기기에서 단어를 판정합니다. 서버는 학습 이벤트를 Kafka와 Spark로 처리해 MongoDB 원장과 통계로 보관합니다.

MediaPipe 좌표 추출·품질 검사와 kNN 학습 실험에서 시작해, 영상 비동기 작업·Kafka·Spark 검사를 거쳐 실제 모바일 학습 이벤트를 AWS MongoDB까지 연결했습니다. 개발 과정의 소스, 현재 앱·서버 구조, 실행 근거를 하나의 저장소에서 추적할 수 있도록 구성했습니다.

| 구성 | 구현·수행 내용 |
|---|---|
| 데이터·학습 실험 | MediaPipe 배치 추출, 결측 마스크·손 배정·정규화, 124시퀀스 분할, kNN·특징 비교 |
| 비동기 작업 실습 | 작업 API·MongoDB Outbox·Kafka 요청/결과·Spark 1분 집계·재시작 검사 |
| iOS | SwiftUI 화면, MediaPipe 좌표 추출, Core ML 추론, SceneKit 본보기, 이벤트 재전송 |
| Android | Expo·React Native 앱, WebView 내부 MediaPipe·JavaScript GRU 추론, 이벤트 재전송 |
| 서버 | Caddy HTTPS, FastAPI 이벤트 배치 API, Kafka, Spark Structured Streaming |
| DB·네트워크 | 별도 AWS 계정의 EC2 MongoDB, PrivateLink, TLS 인증, 이벤트 원장·집계 저장 |
| 실제 연동 | 아이폰·안드로이드 학습 기록의 MongoDB 도착과 아이폰 2대 동시 사용 시험 |

핵심 결과는 124시퀀스의 고정 분할 학습·평가, 영상 작업 API 12개·단어 회귀 18개 검사, 두 아이폰 이벤트 17건 저장, MongoDB 검사 문서 73→10, 모바일 엔진 기준 계산 일치입니다. 직접 실행·저장된 과거 결과·담당자 보고를 구분해 [검증 결과](docs/verification.md)와 [측정 근거](evidence/README.md)에 연결했습니다.

## 목차

- [프로젝트 배경과 목표](#프로젝트-배경과-목표)
- [project 구현 내용](#project-구현-내용)
- [사용자 기능](#사용자-기능)
- [앱 구동화면과 예시](#앱-구동화면과-예시)
- [전체 아키텍처](#전체-아키텍처)
- [기술 스택과 선택](#기술-스택과-선택)
- [MediaPipe 개발과 모델 실험](#mediapipe-개발과-모델-실험)
- [모델과 좌표 처리](#모델과-좌표-처리)
- [학습 이벤트 수집](#학습-이벤트-수집)
- [지연 이벤트와 재전송](#지연-이벤트와-재전송)
- [MongoDB 저장과 최적화](#mongodb-저장과-최적화)
- [데이터 모델과 ERD](#데이터-모델과-erd)
- [비동기 작업·복구 검증](#비동기-작업복구-검증)
- [장애 복구와 체크포인트 검증](#장애-복구와-체크포인트-검증)
- [예외 처리와 운영 개선](#예외-처리와-운영-개선)
- [검증 결과](#검증-결과)
- [역할과 협업](#역할과-협업)
- [실행 및 문서](#실행-및-문서)
- [팀 수행 결과](#팀-수행-결과)

## 프로젝트 배경과 목표

처음 수어를 접하는 사용자가 본보기 동작을 보고 따라 하면서 인식 결과를 확인할 수 있는 앱을 만들었습니다. 모바일 추론과 함께 실제 학습 흐름을 기록하는 수집 경로를 구축해, 어떤 단어에서 시도와 실패가 발생하는지 확인할 기반을 마련했습니다.

1. 카메라 입력에서 손·상체 좌표를 추출하고 5개 목표 단어와 기타 동작을 판정합니다.
2. 번역 화면에서 읽은 단어와 후보를 확인하고, 학습 화면에서 본보기를 따라 연습합니다.
3. 연결이 일시적으로 끊겨도 학습 기록을 기기에 보관하고 재전송합니다.
4. 앱 이벤트를 서버·Kafka·MongoDB에서 같은 ID로 추적하고, 집계의 근거를 원장에 남깁니다.

## project 구현 내용

수어 인식·교육 앱의 학습 기록을 실시간 로그 통합 파이프라인으로 연결했습니다. 이벤트 수집부터 스트리밍 처리, 재전송·복구, 저장 조회 최적화까지 구현하고 단계별 결과를 검증했습니다.

| 구현 영역 | 팀 구현·수행 내용 | 근거 |
|---|---|---|
| 실시간 이벤트 수집 | 학습 시작·연습 단어·인식 결과·재시도·완료를 배치 API로 받고 Kafka에 발행 | 앱 소스·서버 담당 보고·실제 이벤트 대조 |
| 토픽과 메시지 키 | 학습 이벤트 토픽 사용, user_id를 Kafka 키로 지정, 파티션·오프셋으로 메시지 추적 | 실제 Kafka 조회·서버 담당 보고 |
| 스트리밍 소비·집계 | Spark 상시 소비, 1분 단어별 연습 횟수·최근 5분 미인식 비율·사용자 확인 일치율 계산 | 서버 담당 보고 |
| 체크포인트·재처리 | 체크포인트 기반 소비, 누락 오프셋 4건을 별도 Spark 배치로 복구 | 서버 담당 보고의 총 28건 대조 |
| 재전송 원장 관리 | 동일 event_id 유지, MongoDB _id 고유 키로 개별 기록 관리 | 실제 원장 구조·서버 담당 보고·임시 컬렉션 시험 |
| 저장 조회 최적화 | topic·offset 복합 인덱스로 최근·범위 조회의 문서 검사량 감소 | 실제 AWS 실행 계획·원문 비교 |

학습 지표의 입력은 practice_attempt, recognition_result, answer_selected로 구분합니다. 세부 이벤트 정의는 [데이터 계약](docs/data-contract.md), 수행한 스트리밍 작업은 [수집·처리 문서](docs/streaming-semantics.md)에 정리했습니다.

## 사용자 기능

| 기능 | 동작 |
|---|---|
| 단어 읽기 | 동작 구간을 시작·종료해 판정된 단어와 신뢰도를 표시 |
| 판정 상세 | 후보 확률, 손 검출 상태, 동작 길이 확인 |
| 본보기 학습 | 커피·춥다·우유·감사·자다의 본보기를 보고 따라 하기 |
| 학습 진도 | 목표 단어 인식 결과에 따라 진행 상태 기록 |
| 사용자 확인 | 번역 결과를 확인하거나 다른 단어로 수정한 기록 수집 |
| 자동 동기화 | 기록을 먼저 로컬 큐에 넣고 서버 응답에 따라 삭제·재시도 |

iOS는 SceneKit 3D 본보기를 회전해 볼 수 있습니다. Android는 해당 본보기를 렌더링한 프레임을 재생하며, 회전 가능한 3D 장면은 iOS 구현 범위입니다. 카메라 화면 위에 사용자의 동작을 따라 그리던 실시간 사람 모형은 두 앱에서 제거했습니다.

## 앱 구동화면과 예시

번역 결과 확인부터 본보기 학습·재시도·완료까지, 화면 조작과 학습 이벤트의 연결입니다. **아래 화면은 iPhone 앱 코드로 Mac에서 렌더링한 UI 예시**이며, 실제 기기 캡처·서버 적재 증빙과 구분합니다.

### 번역 · 동작 입력에서 사용자 확인까지

<table>
<tr><th>① 동작 입력</th><th>② 인식 결과</th><th>③ 판정 상세</th><th>④ 불확실한 결과</th></tr>
<tr><td><img src="assets/app-demo/screen-02.png" width="180" alt="번역 동작을 읽는 화면"></td><td><img src="assets/app-demo/screen-03.png" width="180" alt="감사 판정 결과"></td><td><img src="assets/app-demo/screen-04.png" width="180" alt="동작 길이와 손 검출 상태 상세"></td><td><img src="assets/app-demo/screen-05.png" width="180" alt="확실하지 않은 결과와 후보 선택"></td></tr>
<tr><td>0.5~3초 동작 구간</td><td>낱말·신뢰도 표시</td><td>검출 상태·낱말 수정</td><td>후보 확인·다시 읽기</td></tr>
</table>

- **판정 종료:** `recognition_result` 생성. 번역 결과 `read`, 불확실 `unsure`, 짧은 입력 `too_short` 등 구분.
- **사용자 확인·수정:** `answer_selected` 생성. `was_corrected=false`는 기존 후보 확인, `true`는 다른 낱말로 수정.
- **화면 이동:** 결과 상세 열기·카메라 전환·탭 이동 자체는 학습 이벤트 미생성.

### 학습 · 본보기에서 시도·재시도·완료까지

<table>
<tr><th>① 낱말 선택</th><th>② 본보기 학습</th><th>③ 따라 하기 준비</th><th>④ 인식 결과</th></tr>
<tr><td><img src="assets/app-demo/screen-06.png" width="180" alt="학습할 낱말 목록"></td><td><img src="assets/app-demo/screen-07.png" width="180" alt="감사 본보기와 동작 순서"></td><td><img src="assets/app-demo/screen-08.png" width="180" alt="따라 하기 카운트다운"></td><td><img src="assets/app-demo/screen-10.png" width="180" alt="따라 하기 정답 피드백"></td></tr>
<tr><td>5개 목표 낱말</td><td>동작 순서·느린 재생</td><td>3·2·1 준비 후 동작</td><td>결과 확인·반복 연습</td></tr>
</table>

[![학습 화면 조작과 이벤트 발생 흐름](assets/diagrams/app-learning-events.png)](assets/diagrams/app-learning-events.svg)

*이미지를 클릭하면 확대 가능한 SVG 원본 표시.*

*도식은 이벤트 발생 순서의 요약. 상세 화면·예외 분기는 [앱 화면과 이벤트 예시](docs/app-demo.md) 참고.*

### 실제 전송 예시 · 재시도를 포함한 학습 기록 8건

앱 담당자 제공 전송 기록에서 `lesson_started → practice_attempt → recognition_result(unknown) → retry → practice_attempt → recognition_result(correct) → lesson_completed → lesson_ended`의 8건 확인. 이 중 마지막 성공은 누적 세 번째 성공에 해당하며, 아래 발췌가 전체 세션의 모든 시도를 의미하지는 않습니다.

| 관측 구간 | 수치 | 측정 범위 |
|---|---:|---|
| 이벤트 생성 → HTTP 202 수신 | **1.53~1.59초** | iPhone 18 Pro Max 이벤트 8건, 담당자 제공 기록 |
| 앱 배치 대기 설정 | **1.5초** | 앱 코드의 전송 정책 |
| 관측 지연에서 대기 설정을 뺀 잔여 | **0.03~0.09초** | 단순 차감 계산값. 네트워크 RTT·서버 처리시간의 독립 측정값 아님 |
| 시도 시작 → 판정 이벤트 생성 | **5.90초 / 6.06초** | 준비 카운트다운·사용자 동작 등을 포함한 2개 시도 |

**HTTP 202는 Kafka 발행 확인에 따른 접수 응답. MongoDB 저장 완료 응답과 구분.** 이 자료만으로 Kafka→Spark→MongoDB 구간의 지연은 산출하지 않습니다.

[전체 10개 화면·이벤트 대응표·전송 예시 보기](docs/app-demo.md)

## 전체 아키텍처

[![전체 아키텍처: 모바일 인식·이벤트 수집·AWS MongoDB 적재](assets/diagrams/architecture.png)](assets/diagrams/architecture.svg)

*이미지를 클릭하면 SVG 원본을 볼 수 있습니다.*

그림은 실제 학습 이벤트의 적재 경로입니다. 카메라 영상은 기기에서 좌표 추출에 사용하고, 서버에는 학습 이벤트 JSON을 전송합니다.

## 기술 스택과 선택

| 영역 | 기술 | 적용 목적 |
|---|---|---|
| iOS | Swift, SwiftUI, AVFoundation, MediaPipe, Core ML, SceneKit | 카메라 처리, 단말 추론, 학습 UI·본보기 |
| Android | Expo, React Native, WebView, MediaPipe Tasks Vision, JavaScript | Android에서 같은 좌표 규격과 모델 계산 구현 |
| API | Python, FastAPI | 이벤트 종류별 입력 검증, 인증, 배치 수신 |
| HTTPS | Caddy, Let’s Encrypt | 모바일 요청의 전송 암호화 |
| 메시지 | Apache Kafka | 이벤트 버퍼링, 소비 위치 추적, 재처리 기반 |
| 스트림 처리 | Spark Structured Streaming | 이벤트 소비, 시간 창 집계, 체크포인트 |
| 저장 | MongoDB, PyMongo | 이벤트 ID 기반 원문 보관과 집계 조회 |
| AWS | EC2, Elastic IP, PrivateLink, Internal NLB | 실행 환경, 고정 관리 주소, 계정 간 사설 접속 |
| 실행 관리 | Docker, Docker Compose | API·메시지·스트림 작업의 실행 환경 관리 |

기술별 배포 구조와 판단은 [아키텍처 문서](docs/architecture.md)에 정리했습니다.

처음 보는 독자는 [데이터 흐름도](docs/data-flow.md)에서 앱 내부 처리, 서버 수락, MongoDB 적재, 오류 조사 순서를 볼 수 있습니다.

## MediaPipe 개발과 모델 실험

카메라 검출 결과를 곧바로 학습시키지 않고, 원시 좌표·손 배정·정규화·시간 길이·결측 마스크·분할을 단계별로 검사하는 코드를 작성했습니다.

[![MediaPipe 개발: 원시 좌표·손 배정·정규화·학습 입력·모델 평가](assets/diagrams/mediapipe-development.png)](assets/diagrams/mediapipe-development.svg)

*이미지를 클릭하면 SVG 원본을 볼 수 있습니다.*

| 기술 구현 | 구현 내용 | 코드 |
|---|---|---|
| 녹화 시각·파일 관리 | 실제 프레임 시각으로 FPS 계산, 프레임 크기 검사·임시 파일 저장 | [record_webcam_clip.py](experiments/03_실습코드/record_webcam_clip.py) |
| 손 슬롯 안정화 | 라벨·이전 손목 위치로 배정, 모호한 후보 보류 | [track_hands_v2_experiment.py](experiments/03_실습코드/track_hands_v2_experiment.py) |
| 좌표·결측 분리 | 원시 XYZ·NaN·검출 마스크와 학습용 0 채움을 구분 | [build_mediapipe177_pilot.py](experiments/03_실습코드/build_mediapipe177_pilot.py) |
| 기준 좌표 정규화 | 어깨 중심·너비 정규화와 복원 오차 검사 | [normalize_mediapipe177_pilot.py](experiments/03_실습코드/normalize_mediapipe177_pilot.py) |
| 시간 길이 통일 | 30개 프레임을 선택하고 원본 인덱스·시각 보존 | [resample_xy30_pilot.py](experiments/03_실습코드/resample_xy30_pilot.py) |
| 학습 입력 검증 | 124개 시퀀스·라벨·분할·배열·파일 해시 대조 | [prepare_mediapipe_baseline.py](experiments/03_실습코드/prepare_mediapipe_baseline.py) |
| 모델 선택·평가 | validation에서 k 선택, test 고정 평가·혼동 행렬·저장 후 재예측 | [train_aihub_knn.py](experiments/03_실습코드/train_aihub_knn.py) |

### 학습 실험의 결과와 해석

촬영 코드 그룹으로 train 76·validation 16·test 32를 분리하고, k=1·3·5 중 validation 기준으로 선택했습니다.

| 기준선 | 입력 | validation | test | test macro F1 |
|---|---|---:|---:|---:|
| AIHub 2D kNN | 30×150 | 14/16, 87.50% | 30/32, 93.75% | 0.9370 |
| MediaPipe kNN | 30×177, XY+마스크 | 15/16, 93.75% | 31/32, 96.875% | 0.9683 |

두 결과는 동일한 8개 임시 클래스·고정 분할 실험의 저장 결과입니다. 현재 앱의 r0006 GRU 성능 수치와 구분합니다. 촬영 코드 분할의 단위를 사람 독립 평가로 확대 해석하지 않습니다.

**입력 차원뿐 아니라 특징의 의미를 관리했습니다.** 실험의 177값은 XY 118+마스크 59이고 모바일 모델은 59점 XYZ입니다. 손목 상대화·손목 위치 추가 실험은 183값입니다. 같은 크기의 배열도 전처리 버전·손 슬롯·좌표계가 다르면 별도 계약으로 다룹니다.

[MediaPipe 구현 상세](docs/mediapipe-development.md) · [학습·평가 상세](docs/model-development.md) · [학습 결과 JSON](evidence/model-development.json)

## 모델과 좌표 처리

| 항목 | 현재 구현 |
|---|---|
| 모델 | `r0006`, GRU 기반 단어 분류 |
| 출력 순서 | 기타 → 커피 → 춥다 → 우유 → 감사 → 자다 |
| 프레임 규격 | 양손 21점씩 + 상체 17점 = 59점, 각 점의 x·y·z로 177개 값 |
| 입력 | 선택한 동작을 30프레임으로 재표본화한 `30 × 177` |
| 전처리 | 화면 비율 반영, 어깨 중심·너비 기준 정규화, 미검출 입력 처리 |
| 구간 | 약 0.5~3초, 번역 버튼 방식과 따라 하기 자동 구간 처리 |
| iOS 추론 | Core ML 모델 |
| Android 추론 | 변환한 가중치를 사용하는 JavaScript GRU 계산 |
| 표시 기준 | 제공 코드의 확신 표시 임시 기준 `0.6`, 동작 손 미검출 비율 기준 `0.5` |

표의 확률 기준은 제공 앱 코드에서 사용하는 표시용 기준입니다. 모델 확률과 실측 정확도는 구분합니다.

좌우를 뒤집은 좌표도 판정하는 보완 로직이 포함돼 있습니다.

### 전처리 이식과 기준값 검사

30프레임 재표본화는 구간의 프레임 인덱스를 균등 선택하는 방식입니다. 화면 비율로 x·z 단위를 맞추고 어깨 중심·너비로 정규화합니다. 어깨 미검출은 같은 시퀀스의 유효 평균으로 처리하고 손 z에는 손목 기준 좌표라는 예외를 적용합니다.

Android 엔진·모델의 기준 데이터 1건으로 전처리와 확률을 재계산했습니다. 전처리 최대 절대 오차는 **2.384×10⁻⁷**, 확률 최대 절대 오차는 **7.645×10⁻⁸**로 제공 테스트의 허용 기준을 통과했고, 기대 단어 ‘감사’도 일치했습니다. 이 수치는 제공 단일 기준 입력의 계산 일치 결과입니다.

[전처리 수식·좌우 선택·엔진 계산](docs/app-engine.md) · [재검증 결과 JSON](evidence/android-engine-reference.json)

## 학습 이벤트 수집

앱은 재전송에서도 같은 `event_id`를 유지합니다. API는 최대 100건의 이벤트 배치를 검증하고 Kafka 발행 성공을 확인한 뒤 HTTP `202`를 반환합니다. `202`는 Kafka 발행 확인 시점의 수락을 뜻하며, MongoDB 적재는 뒤따르는 비동기 작업에서 확인합니다.

| 기록 | 용도 |
|---|---|
| `lesson_started` | 학습 시작 |
| `practice_attempt` | 연습 시도 |
| `recognition_result` | 단어 판정·미인식 결과 |
| `retry` | 실패 후 재시도 |
| `lesson_completed` | 목표 학습 완료 |
| `lesson_ended` | 학습 종료 |
| `answer_selected` | 사용자의 번역 결과 확인·수정 |

서버는 `event_time`, `sent_at`, `received_at`을 보존합니다. 폰에서 발생한 시각과 실제 수신 시각을 구분해 오프라인 기록의 재전송을 추적합니다. Kafka 메시지 키는 `user_id`, DB 원장 문서 키는 `event_id`입니다.

[이벤트 계약과 저장 구조](docs/data-contract.md)

## 지연 이벤트와 재전송

모바일에서는 이벤트가 발생한 시각과 서버가 받은 시각이 다를 수 있습니다. 팀은 기록을 먼저 Outbox에 보관하고, 재전송에서도 같은 event_id와 발생 시각을 유지했습니다. API는 전송·수신 시각을 함께 보존해 뒤늦게 도착한 기록을 원장에서 대조할 수 있게 했습니다.

| 구분 | 필드 | 사용 목적 |
|---|---|---|
| 앱 동작 발생 | event_time | 실제 학습 행동이 일어난 시각 |
| 배치 전송 | sent_at | 앱이 기록 묶음을 보낸 시각 |
| 서버 수신 | received_at | API가 기록 묶음을 받은 시각 |
| 재전송 식별 | event_id | 이미 보낸 기록과 새 기록 구분 |

### 실제 아이폰의 지연 도착 대조

폰에 대기 중이던 기록 9건이 고정 주소 빌드 설치 후 전송됐습니다. 앱 담당자의 전송 로그와 DB 담당자의 원장 조회를 연결했습니다. 아래 시각은 한국 시간입니다.

| 관측 항목 | 실제 확인 내용 |
|---|---|
| 기록 발생 시각 | 09:06~09:07 |
| 서버 수신 시각 | 09:13:01 |
| 앱 전송 기록 | 순번 164~172의 9건, 모두 HTTP 202 |
| MongoDB 대조 | 오프셋 578~586의 9건 저장 |
| 이어진 학습 | 09:14에 감사 학습 8건 전송·모두 202, 원장 587~594와 연결 |

[![지연 도착 기록: 발생·Outbox 대기·서버 수신·원장 대조](assets/diagrams/late-arrival.png)](assets/diagrams/late-arrival.svg)

*이미지를 클릭하면 SVG 원본을 볼 수 있습니다.*

이 사례는 발생 후 수분 뒤 도착한 이벤트의 전송·원장 저장을 확인한 결과입니다. 발생→수신 시각 차이에는 폰의 대기 시간이 포함되므로 Spark 처리 지연이나 DB 쓰기 시간으로 환산하지 않습니다. 지연 도착 기록의 적재 대조와 시간 창 통계의 계산 의미를 구분해 제시합니다.

[앱 전송 확인](docs/app-status.md#실제-폰-전송과-원장-연결) · [DB 관측 기록](docs/verification.md#실제-aws-mongodb-대조) · [시각 필드 계약](docs/data-contract.md#식별자와-시각)

## MongoDB 저장과 최적화

실제 이벤트 원장 `learning_attempt_test_app_events`의 구조는 다음과 같습니다.

| 필드 | 자료형 | 역할 |
|---|---|---|
| `_id` | 문자열 | 이벤트 ID, 문서 중복 방지 기준 |
| `topic` | 문자열 | Kafka 토픽 |
| `offset` | 정수 | Kafka 처리 위치 |
| `payload` | 문자열 | JSON 형식의 이벤트 원문과 수신 메타데이터 |

`payload`는 문자열이므로 내부의 사용자·기기 값을 조회하려면 JSON 해석이 필요합니다. 원장과 집계 스냅샷은 별도 컬렉션으로 관리합니다. 아래 물리 저장 구조와 인덱스 비교는 직접 조회한 이벤트 원장을 기준으로 합니다.

최근 기록 조회에 `topic_offset_desc` 복합 인덱스 `{ topic: 1, offset: -1 }`를 적용했습니다. 원장 73건에서 최근 10건을 조회한 실행 계획이 전체 스캔·별도 정렬에서 인덱스 탐색으로 바뀌었습니다.

| 비교 | 적용 전 | 적용 후 |
|---|---|---|
| 문서 검사 | 73건 | 10건 |
| 반환 | 10건 | 10건 |
| 별도 정렬 단계 | 있음 | 없음 |
| 반환 원문·순서 대조 | 기준 | 일치 |

문서 검사량은 약 86.3% 감소했습니다. 당시 실행 시간은 0~1ms로, 지연 시간 개선률을 산정하기에는 데이터와 반복 측정이 충분하지 않습니다.

## 데이터 모델과 ERD

데이터 모델은 **실제 저장 구조**와 **앱 이벤트의 논리적 연결**을 나누어 표현했습니다. 위쪽은 MongoDB 원장에 직접 저장되는 네 필드, 아래쪽은 payload 내부 식별자를 통해 연결되는 사용자·세션·학습·인식 관계입니다.

[![데이터 모델과 ERD: MongoDB 물리 원장과 앱 이벤트의 논리적 관계](assets/diagrams/data-model.png)](assets/diagrams/data-model.svg)

*이미지를 클릭하면 SVG 원본을 볼 수 있습니다.*

- **물리 저장:** _id는 event_id 기반 고유 키이며 topic·offset·payload로 원천 위치와 원문을 보관합니다.
- **논리 관계:** user_id·session_id·lesson_id·reading_id로 이벤트를 연결합니다. 이벤트 종류에 따라 해당 식별자를 사용합니다.
- **집계:** 여러 원장 이벤트에서 학습 통계를 계산합니다. 논리적 연결은 별도 사용자·학습 컬렉션이나 외래 키 제약을 뜻하지 않습니다.

[실제 필드·논리 관계·실습 원장 상세](docs/data-model.md)

## 비동기 작업·복구 검증

실제 모바일 로그 경로와 별도로, 영상·특징 입력을 비동기 작업으로 처리하는 실습 코드를 구현했습니다. 작업과 Outbox를 같은 MongoDB 문서에 기록하고 Kafka의 requested·completed·failed 토픽으로 처리 단계를 연결했습니다.

Spark 시제품은 foreachBatch에서 이벤트의 종류·버전·필수 ID를 검사하고, event_id 고유 원장·fingerprint로 재전달을 처리합니다. 고유 원장에서 통계 절대값을 재계산하고 bulk upsert로 갱신합니다. 1분 집계는 F.window를 사용하며 실습 시간 기준은 Kafka publish timestamp입니다.

| 확인한 동작 | 저장된 결과 |
|---|---|
| 영상 접수·동일 요청·잘못된 구간·없는 파일 | HTTP 201·202·200·409·422·404 등 12개 검사 통과 |
| 완료 이벤트 3회 재전달 | 고유 원장·누적·1분 창과 원장 시각 유지 |
| 우유·감사·춥다 고정 영상 | 단어당 6개, 총 18개 회귀 검사 통과 |
| 워커 중단 후 재개 | 대기 작업의 동일 ID·결과·추론 1회 유지 |
| Spark 중단 후 재개 | 같은 체크포인트에서 중단 중 쌓인 1건 반영·기존 기록 보존 |

이 실습의 녹화·영상 업로드는 개발 검사용 경로입니다. 현재 모바일 앱은 기기 내 추론 후 학습 로그를 전송합니다.

[Kafka·Spark·작업 상태·복구 상세](docs/prototype-pipeline.md) · [실행 결과 요약](evidence/pipeline-regression.json)

## 장애 복구와 체크포인트 검증

장애 복구는 작업 API·워커·Kafka·Spark·원장을 나누어 확인했습니다. 영상 작업 실습에서는 한 구성 요소를 중단해도 다른 단계가 진행되는지 관측하고, 재개 후 기존 기록과 새 기록을 대조했습니다. 실제 모바일 서버의 재기동·누락 복구는 서버 담당자의 별도 보고로 연결합니다.

### 영상 작업 실습의 중단·재개

| 단계 | 중단 중 관측 | 재개 후 확인 |
|---|---|---|
| 워커 중단 | API 접수·요청 발행 성공, 5초간 queued·추론 0회 | 같은 작업 ID로 완료, Kafka 결과 발행·Spark 반영 |
| 같은 요청 재전송 | client_request_id로 기존 작업 조회 | 작업 ID·결과·추론 1회 유지 |
| Spark 중단 | API·워커·Kafka 결과 발행 성공, 5초간 기존 통계 유지 | 중단 중 쌓인 작업 1건을 원장·누적·1분 창·완료 통계에 반영 |
| 같은 체크포인트 재개 | 기존 원장·fingerprint·시각·checkpoint 상태를 기준으로 보존 | 기존 기록 유지, 완료 이벤트 3회 재전달 후 통계 유지 |

검사 기록은 queue-worker → finish-worker → queue-spark → finish-spark의 네 단계로 구성되며, 단계별 확인 항목은 3개입니다. 표의 5초는 **중단 상태의 관측 시간**입니다. 복구 완료 시간이나 지연 보장값을 뜻하지 않습니다.

### 복구를 뒷받침하는 구현

- **작업 멱등 키:** client_request_id 고유 인덱스와 입력 fingerprint로 정상 재요청을 기존 작업에 연결합니다.
- **원장 고유성:** event_id 기반 _id와 sequence_id 고유 인덱스, 내용 fingerprint를 사용합니다.
- **멱등 집계 출력:** 원장의 고유 이벤트로 통계 절대값을 재계산하고 ReplaceOne(upsert=True)·bulk_write로 갱신합니다.
- **소비 위치 복구:** Spark checkpointLocation을 유지하고 broker·prefix·database 바인딩을 검사합니다.
- **전후 대조:** 원장 건수만이 아니라 기존 원문의 fingerprint·기록 시각과 단어별 누적·1분 창·상태 통계를 비교합니다.

이 구현과 복구 결과는 로컬 단일 워커 기반 영상 실습의 검증 범위입니다. 워커 대기·재개와 Spark 소비 재시작을 각각 나눠 확인했습니다.

### 실제 모바일 서버의 복구 보고

| 운영 시나리오 | 보고된 결과 | 근거 |
|---|---|---|
| 누락 범위 재처리 | Kafka 437~440의 4건을 별도 Spark 배치로 복구, 기존 24건을 포함한 총 28건 대조 | 서버 담당 제공 보고 |
| EC2 중지·시작 | PrivateLink MongoDB ping 정상, 오프셋 561~576의 이벤트 16건 저장 | 서버 담당 제공 보고 |
| 상시 저장 | 아이폰 이벤트 504~560의 57건, ID·오프셋·원문 일치 | 서버 담당 제공 보고 |

위 범위는 서로 겹칠 수 있으므로 합산해 전체 처리 건수로 사용하지 않습니다.

[복구 검사 코드](experiments/02_파이프라인/video_recovery_checks_v0.1_20261109/test_video_recovery.py) · [원장·집계 코드](experiments/02_파이프라인/video_spark_stats_v0.1_20261109/spark_video_stats.py) · [저장된 회귀·복구 결과](evidence/pipeline-regression.json) · [서버 배포·검증 보고](server/aws-deployment-report.md)

## 예외 처리와 운영 개선

| 문제 | 구현·조치 | 근거 |
|---|---|---|
| 오프라인·일시 통신 실패 | 로컬 선저장, event_id 유지, 2초부터 최대 30초까지 지수 백오프 | iOS 코드 확인, 실제 재송신 적재 관측 |
| 배치 안 일부 입력 오류 | 거절 배치를 개별 전송해 정상 이벤트를 수락하고 오류 이벤트를 분리 | iOS Outbox 코드 확인 |
| 잘못된 API 입력·인증 | 종류별 필드·최대 100건·본문 1MB·X-API-Key 검증 | 서버 보고, 400·401 관측 |
| 응답 유실 후 같은 기록 재전송 | event_id를 MongoDB _id로 사용 | 실제 저장 구조, 중복 방지 서버 보고 |
| 누락된 이벤트 | 특정 Kafka 오프셋을 별도 배치로 재처리 | 서버 보고의 4건 복구·총 28건 대조 |
| 최근 조회 전체 스캔 | topic·offset 복합 인덱스 적용 | 실제 AWS 문서 검사 73→10 |
| DB 입력 예외 | 재시도·고유 키 오류·스키마 검증 | 임시 컬렉션 합성 시험 |
| 서버 재시작·주소 변경 | 컨테이너 재시작 정책, 체크포인트, Elastic IP | 서버 보고와 DB 관리 주소 확인 |

적용한 처리의 목적과 검증 조건을 [예외 처리·최적화 상세](docs/reliability-and-optimization.md)에 정리했습니다.

### 수신·저장의 책임 경계

- iOS는 파일 선저장, 동시에 여러 flush가 실행되지 않도록 하는 상태 가드, 원자적 파일 쓰기를 사용합니다.
- 일시 통신 실패는 2초부터 최대 30초까지 대기하며 같은 ID로 다시 보냅니다. 400·413·422는 거절 상태로 관리하고 배치를 개별 요청으로 분리합니다.
- API 수락은 Kafka 발행 확인 시점입니다. 응답 유실로 이미 발행된 이벤트가 다시 도착할 수 있습니다.
- 원장에는 event_id를 _id로 저장하고, Spark는 체크포인트를 사용해 소비 위치를 관리합니다.
- 전송 이력의 event_id·HTTP 응답을 Kafka 위치와 DB 원문에 연결해 실패 구간을 좁힙니다.

각 선택의 효과와 비용은 [설계 결정](docs/design-decisions.md), 실제 장애 조사 순서는 [운영 절차](docs/operations.md)에 정리했습니다.

## 검증 결과

| 검증 | 결과 | 근거 범위 |
|---|---|---|
| 실제 아이폰 이벤트 | 전달받은 이벤트 ID·오프셋 11건 일치, 이후 13건 추가 대조 일치 | DB 담당 직접 조회 |
| 아이폰 2대 동시 사용 | 시작 요청 수신 시각 차이 0.223초, 두 사용자·세션의 기록 총 17건 저장 | DB 담당 직접 조회 |
| Android 연동 | 오프셋 570~576의 이벤트 7건 저장, `device=android-16` 확인 | DB 담당 직접 조회 |
| 재전송 관찰 | 대기 기록 9건이 09:13:01에 수신돼 원장에 저장, 이후 8건 추가 저장 | DB 담당 직접 조회 |
| 원장 조회 최적화 | 문서 검사 73→10, 반환 문서 BSON 해시·순서 일치 | DB 담당 실행 계획·문서 비교 |
| 재시도·잘못된 입력 | 임시 컬렉션에서 재전송 한 건 유지, 원본 보존, 오류 11000·121 확인 | DB 담당의 별도 합성 시험 |
| 누락 재처리 | 누락 4건 재처리 후 기존 24건 포함 총 28건 대조 일치 | 서버 담당 제공 보고 |
| 상시 Spark 저장 | 오프셋 504~560의 아이폰 이벤트 57건이 MongoDB에 자동 저장됐고, 이벤트 ID·오프셋·원문이 일치 | 서버 담당 제공 보고 |
| Android 기준 계산 재검증 | 전처리 5,310값·6확률의 최대 오차 허용 범위 통과 | 제공 코드·단일 fixture 직접 실행 |

위 검증 집합은 일부 겹칩니다. 합산해 전체 적재 건수로 사용하지 않습니다. 두 대 동시 시험은 해당 시나리오의 저장 성공을 검증한 결과입니다.

[검증 방법·수치·한계](docs/verification.md) · [Kafka 수신 캡처](server/README.md#kafka-수신-확인)

## 역할과 협업

| 팀원 | 담당 영역 | 주요 구현·수행 내용 |
|---|---|---|
| **신경호** | 데이터·파이프라인 개발 및 DB | 녹화·MediaPipe 좌표 처리·손 배정·특징 변환·kNN 실험, 영상 작업·Kafka·Spark·복구 실습, MongoDB·PrivateLink·TLS 연결, 원장 대조·조회 인덱스·DB 예외 시험 |
| **이종연** | 메인 서버 | AWS EC2·Docker·Caddy HTTPS, FastAPI 인증·입력 검증, Kafka·Spark 상시 작업, 학습 지표·재처리·재시작 운영 |
| **김형준** | 앱 개발 | iOS·Android 앱, MediaPipe·r0006 추론 연동, 전처리·학습 UI·본보기, Outbox·재전송, 이벤트 규격·실기기 전송 확인 |

협업의 공통 추적 키는 event_id입니다. 앱의 전송 로그와 HTTP 응답, 서버의 Kafka 파티션·오프셋, DB 원장의 ID·원문을 함께 대조해 수신·처리·저장 구간을 검증했습니다.

## 실행 및 문서

이 패키지는 모바일 소스·실습 코드·실행 구성·서버 배포 보고·기술 문서·측정 근거로 구성됩니다. 경로별 역할과 의존성은 [소스 안내](docs/source-map.md)에 있습니다.

| 문서 | 내용 |
|---|---|
| [소스·실행 안내](docs/source-map.md) | 실험·모바일·서버 코드 경계, 실행 단위, 출처·해시 |
| [MediaPipe 개발](docs/mediapipe-development.md) | 손 배정·결측·정규화·30프레임·입력 계약 |
| [학습·평가](docs/model-development.md) | 124개 분할·kNN·특징 비교·평가 해석 |
| [비동기 작업·복구](docs/prototype-pipeline.md) | 작업 API·Outbox·Kafka·1분 집계·중단 복구 |
| [project 구현 요약](docs/instructor-requirements.md) | 수집·처리·재전송·복구·최적화의 구현 근거 |
| [스트리밍 처리 명세](docs/streaming-semantics.md) | Kafka 토픽·키, Spark 소비·집계·체크포인트, 누락 재처리 |
| [메인 AWS 서버](server/README.md) | EC2 배포, FastAPI부터 MongoDB까지의 처리, Kafka 수신 캡처, 누락 복구 |
| [아키텍처](docs/architecture.md) | 앱 추론·수집·계정 간 연결·설계 판단 |
| [앱 엔진](docs/app-engine.md) | 좌표 슬롯·전처리 수식·좌우 보완·기준값 재검증 |
| [앱 담당자 확인 결과](docs/app-status.md) | 최신 빌드·재전송 확인, 동의·수집 범위, 기기 값, 앱 묶음 정리, 보안 점검 |
| [데이터 계약](docs/data-contract.md) | 이벤트·시각·식별자·DB 필드 |
| [데이터 모델·ERD](docs/data-model.md) | 실제 저장 구조와 이벤트의 논리 관계 |
| [데이터 흐름도](docs/data-flow.md) | 앱 처리, 요청 수락, 적재, 오류 조사 시각화 |
| [예외 처리·최적화](docs/reliability-and-optimization.md) | 코드별 예외 분기, 재시도, 재처리, 실행 계획 개선 |
| [설계 결정](docs/design-decisions.md) | 선택한 구조의 효과·비용·검증 경계 |
| [운영 절차](docs/operations.md) | 수신·발행·소비·저장의 조사 순서와 단건 조회 |
| [측정 근거](evidence/README.md) | DB 실행 결과와 Android 기준 계산 결과 |
| [검증 결과](docs/verification.md) | 실제 시험, 제공 보고, 실행 계획과 한계 |
| [발표 진행안](docs/presentation.md) | 팀 발표 흐름·시연 순서·예상 질문 |
| [게시 검토](docs/release-review.md) | 파일·권리·설정 정리와 근거 목록 |

iOS 코드는 mobile/ios, Android 코드는 mobile/android, Python 실험은 experiments에 있습니다. 모델·본보기·데이터 자원은 원본 이용 조건에 따라 별도로 관리합니다. 코드 스냅샷과 최신 앱 담당자 확인의 시점은 [소스 안내](docs/source-map.md), 기술 자료와 파일 구성은 [게시 범위](docs/release-review.md)에 정리했습니다.

## 팀 수행 결과

- MediaPipe 원시 좌표·손 배정·품질·정규화·프레임 선택과 124개 학습 데이터 구성
- NumPy kNN·손목 상대 특징 실험, 고정 분할 평가·모델 저장 재검증
- 영상 비동기 API·MongoDB Outbox·Kafka·Spark 1분 통계·중단 복구 실습

- iOS·Android 앱과 실제 모바일 학습 이벤트의 AWS MongoDB 적재 확인
- HTTPS API → Kafka → 상시 Spark → PrivateLink → MongoDB 연결
- 학습 지표 계산과 체크포인트 운영에 대한 서버 구현 보고 통합
- 이벤트 ID·Kafka 오프셋·원문 대조와 두 아이폰 동시 사용 시험
- 누락 오프셋의 Spark 배치 재처리 및 원장 대조
- 실제 MongoDB 복합 인덱스 적용, 원문 보존 확인, 임시 컬렉션 예외 시험
- 메인·DB EC2 탄력적 IP 적용과 팀 인수인계
