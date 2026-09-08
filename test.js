// ================================================================
// ===== КОНФИГУРАЦИЯ =====
// ================================================================
const API_CONFIG = {
  baseUrl: '/api/v1/',
  headers: {
    'Accept': 'application/json',
    'Content-Type': 'application/json'
  }
};

// ================================================================
// ===== ПЕРЕМЕННЫЕ СОСТОЯНИЯ =====
// ================================================================
let autoRefreshInterval = null;
let taskDataCache = {};
let offsetCompletedTask = 0;

let choosedDagId = 'qwe';
const ArmDagId = 'qwe';
const CrawDagId = 'qwe_gid';

let currentSearchParams = {
  vendor: '',
  model: '',
};

let selectedDags = new Set();
let firstCardSelected = false;
let alertsMap = {};

// ===== ПЕРЕМЕННЫЕ ДЛЯ ПАГИНАЦИИ =====
let activeOffset = 0;
let activeLimit = 20;
let activeTotal = 0;
let activeLoading = false;
let activeHasMore = true;
let accessToken = null;

// ================================================================
// ===== DOM ЭЛЕМЕНТЫ =====
// ================================================================
const elements = {
  toggleMode: document.getElementById('modeToggle'),
  dagList: document.getElementById('dag-list'),
  completedDagList: document.getElementById('completed-dag-list'),
  refreshBtn: document.getElementById('refreshBtn'),
  autoRefreshCheckbox: document.getElementById('autoRefreshCheckbox'),
  loadMoreBtn: document.getElementById('loadMoreBtn'),
  loadingIndicator: document.getElementById('loadingIndicator'),
  CompletedLoadingIndicator: document.getElementById('completedLoadingIndicator'),
  activeDagsCount: document.getElementById('active-dags-count'),
  completedDagsCount: document.getElementById('completed-dags-count'),
  completedTab: document.getElementById('completed-tab'),
  divForStatusMessages: document.getElementById('divForStatusMessages'),

  searchVendor: document.getElementById('searchVendor'),
  searchModel: document.getElementById('searchModel'),
  searchBtn: document.getElementById('searchBtn'),
  searchResultsInfo: document.getElementById('searchResultsInfo'),
  searchCriteria: document.getElementById('searchCriteria'),
  clearSearch: document.getElementById('clearSearch'),

  runningCount: document.getElementById('running-count'),
  successCount: document.getElementById('success-count'),
  failedCount: document.getElementById('failed-count'),
  queuedCount: document.getElementById('queued-count'),

  runningSpinner: document.getElementById('running-spinner'),
  successSpinner: document.getElementById('success-spinner'),
  failedSpinner: document.getElementById('failed-spinner'),
  queuedSpinner: document.getElementById('queued-spinner'),

  selectionControls: document.getElementById('selectionControls'),
  selectAllCheckbox: document.getElementById('selectAllCheckbox'),
  selectedCount: document.getElementById('selectedCount'),
  deleteSelectedBtn: document.getElementById('deleteSelectedBtn'),
  clearSelectionBtn: document.getElementById('clearSelectionBtn'),

  // ===== ДЛЯ БЕСКОНЕЧНОГО СКРОЛЛА =====
  loadingMoreIndicator: document.getElementById('loadingMoreIndicator'),
  scrollTrigger: document.getElementById('scrollTrigger')
};

// ================================================================
// ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====
// ================================================================
function formatDateTime(dateString) {
  if (!dateString) return 'N/A';
  const date = new Date(dateString);
  return date.toLocaleString();
}

function formatDuration(seconds) {
  if (!seconds) return 'N/A';
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);
  return [hours > 0 ? `${hours}h` : null,
    minutes > 0 ? `${minutes}m` : null,
    `${secs}s`].filter(Boolean).join(' ');
}

// ===== СТАТУСЫ (ВСЕ 12 ШТУК) =====
function getStatusBadgeClass(state) {
  const classes = {
    running: 'bg-primary bg-opacity-10 text-primary',
    success: 'bg-success bg-opacity-10 text-success',
    failed: 'bg-danger bg-opacity-10 text-danger',
    queued: 'bg-warning bg-opacity-10 text-warning',
    upstream_failed: 'bg-danger bg-opacity-10 text-danger',
    skipped: 'bg-secondary bg-opacity-10 text-secondary',
    scheduled: 'bg-info bg-opacity-10 text-info',
    up_for_retry: 'bg-warning bg-opacity-10 text-warning',
    up_for_reschedule: 'bg-warning bg-opacity-10 text-warning',
    deferred: 'bg-secondary bg-opacity-10 text-secondary',
    removed: 'bg-danger bg-opacity-10 text-danger',
    restarting: 'bg-primary bg-opacity-10 text-primary'
  };
  return classes[state] || 'bg-secondary bg-opacity-10 text-secondary';
}

function getTaskStateInfo(state) {
  const states = {
    success: { icon: "bi-check-circle-fill", color: "text-success" },
    running: { icon: "bi-play-fill", color: "text-primary" },
    failed: { icon: "bi-exclamation-triangle-fill", color: "text-danger" },
    queued: { icon: "bi-hourglass", color: "text-warning" },
    upstream_failed: { icon: "bi-arrow-left-circle-fill", color: "text-danger" },
    skipped: { icon: "bi-skip-forward-circle-fill", color: "text-secondary" },
    scheduled: { icon: "bi-clock", color: "text-info" },
    up_for_retry: { icon: "bi-arrow-repeat", color: "text-warning" },
    up_for_reschedule: { icon: "bi-calendar-event", color: "text-warning" },
    deferred: { icon: "bi-pause-circle", color: "text-secondary" },
    removed: { icon: "bi-x-circle", color: "text-danger" },
    restarting: { icon: "bi-arrow-clockwise", color: "text-primary" }
  };
  return states[state] || { icon: "bi-question-circle", color: "text-muted" };
}

function getTriggerType(dagRun) {
  if (dagRun.external_trigger === true) {
    if (dagRun.conf && dagRun.conf.parent_dag_id) {
      return { type: 'parent', label: 'Parent', icon: 'bi-diagram-3' };
    }
    return { type: 'manual', label: 'Manual', icon: 'bi-hand-index' };
  }
  return { type: 'scheduled', label: 'Scheduled', icon: 'bi-clock' };
}

function getStatusIcon(state) {
  const map = {
    running: 'bi-play-fill text-primary',
    success: 'bi-check-circle-fill text-success',
    failed: 'bi-exclamation-triangle-fill text-danger',
    queued: 'bi-hourglass text-warning'
  };
  return map[state] || 'bi-question-circle text-muted';
}

// ================================================================
// ===== API ФУНКЦИИ =====
// ================================================================
async function getToken() {
  try {
    const response = await fetch('/api/token');
    const data = await response.json();
    accessToken = data.access_token;
    return accessToken;
  } catch (error) {
    console.error('Error getting token:', error);
    throw error;
  }
}

async function fetchApi(endpoint, method = 'GET') {
  const option = {
    method,
    headers: API_CONFIG.headers,
  };

  if (!accessToken) {
    await getToken();
  }

  option.headers["Authorization"] = `Bearer ${accessToken}`;

  try {
    const response = await fetch(endpoint, option);
    if (!response.ok) {
      throw new Error(`API request failed with status ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    console.error('API request error:', error);
    throw error;
  }
}

async function fetchAllDagRuns(dag_id) {
  const response = await fetchApi(`${API_CONFIG.baseUrl}dags/${dag_id}/dagRuns`);
  return response.dag_runs;
}

async function fetchTasks(dagId, dagRunId) {
  const cacheKey = `${dagId}-${dagRunId}`;
  if (taskDataCache[cacheKey]) {
    return taskDataCache[cacheKey];
  }

  const endpoint = `${API_CONFIG.baseUrl}dags/${encodeURIComponent(dagId)}/dagRuns/${encodeURIComponent(dagRunId)}/taskInstances`;
  const response = await fetchApi(endpoint);
  taskDataCache[cacheKey] = response.task_instances;
  return response.task_instances;
}

async function fetchDagRunDetail(dagId, dagRunId) {
  const endpoint = `${API_CONFIG.baseUrl}dags/${encodeURIComponent(dagId)}/dagRuns/${encodeURIComponent(dagRunId)}`;
  return await fetchApi(endpoint);
}

async function fetchCompletedTasks(offset) {
  let endpoint = null;
  if (currentSearchParams.vendor && currentSearchParams.model) {
    endpoint = `/query/vmsearch?index=ots_analyze_file_firmware_fs_structure&field=firmware_vendor.value,firmware_model.value&query=[${currentSearchParams.vendor}],[${currentSearchParams.model}]&offset=${offset}`;
  } else if (currentSearchParams.vendor) {
    endpoint = `/query/vendorsearch?index=ots_analyze_file_firmware_fs_structure&field=firmware_vendor.value&query=${currentSearchParams.vendor}&offset=${offset}`;
  } else if (currentSearchParams.model) {
    endpoint = `/query/modelsearch?index=ots_analyze_file_firmware_fs_structure&field=firmware_model.value&query=${currentSearchParams.model}&offset=${offset}`;
  } else {
    endpoint = `/query/allsearch?index=ots_analyze_file_firmware_fs_structure&exclude=result.value&offset=${offset}`;
  }
  const response = await fetch(endpoint);
  return await response.json();
}

// ================================================================
// ===== ПОИСК CHILD DAG =====
// ================================================================
async function findChildDagRuns(parentDagId, parentDagRunId) {
  try {
    const tasks = await fetchTasks(parentDagId, parentDagRunId);

    const triggerTasks = tasks.filter(t =>
      t.task_type === 'TriggerDagRunOperator' ||
      t.task_type === 'TriggerDagRunOperatorAsync'
    );

    if (triggerTasks.length === 0) return null;

    for (const task of triggerTasks) {
      try {
        const taskDetailEndpoint = `${API_CONFIG.baseUrl}dags/${encodeURIComponent(parentDagId)}/dagRuns/${encodeURIComponent(parentDagRunId)}/taskInstances/${encodeURIComponent(task.task_id)}`;
        const taskDetail = await fetchApi(taskDetailEndpoint);

        const triggeredDagId = taskDetail.conf?.triggered_dag_id || taskDetail.conf?.child_dag_id;
        const triggeredRunId = taskDetail.conf?.triggered_run_id || taskDetail.conf?.child_run_id;

        if (triggeredDagId && triggeredRunId) {
          const childDetail = await fetchDagRunDetail(triggeredDagId, triggeredRunId);
          const childTasks = await fetchTasks(triggeredDagId, triggeredRunId);
          return {
            ...childDetail,
            tasks: childTasks || []
          };
        }
      } catch (e) {
        console.warn(`Could not get child info for task ${task.task_id}:`, e);
      }
    }
    return null;
  } catch (error) {
    console.error('Error finding child DAG runs:', error);
    return null;
  }
}

// ================================================================
// ===== РЕНДЕРИНГ АКТИВНЫХ (С CHILD) =====
// ================================================================
async function renderSingleDagCard(dag) {
  const statusClass = `status-${dag.state}`;
  const statusIcons = {
    running: 'bi-play-fill text-primary',
    success: 'bi-check-circle-fill text-success',
    failed: 'bi-exclamation-triangle-fill text-danger',
    queued: 'bi-hourglass text-warning'
  };

  let detail = null;
  try {
    detail = await fetchDagRunDetail(dag.dag_id, dag.dag_run_id);
  } catch (e) {
    console.warn('Could not fetch detail for', dag.dag_id, e);
  }

  let childRun = null;
  try {
    childRun = await findChildDagRuns(dag.dag_id, dag.dag_run_id);
  } catch (e) {
    console.warn('Could not find child for', dag.dag_id, e);
  }

  const dagItem = document.createElement('div');
  dagItem.id = `${dag.dag_id + dag.dag_run_id}`;
  dagItem.className = `list-group-item p-0 border-0`;

  const dagCard = document.createElement('div');
  dagCard.className = `dag-card p-3 ${statusClass}`;

  const vendor = dag.vendor || dag.dag_id.split('_')[0] || 'DAG';
  const model = dag.model || dag.dag_id.split('_').slice(1).join('_') || '';

  // Child информация
  let childInfoHtml = '';
  if (childRun) {
    const childStatusIcon = getStatusIcon(childRun.state);
    childInfoHtml = `
      <div class="mt-2 child-dag-info">
        <div class="child-header">
          <i class="bi bi-diagram-3 text-primary"></i>
          <strong>Child DAG:</strong>
          <span class="fw-medium">${childRun.dag_id}</span>
          <i class="bi ${childStatusIcon} ms-1"></i>
          <span class="badge ${getStatusBadgeClass(childRun.state)}">${childRun.state.toUpperCase()}</span>
          <span class="dag-run-id ms-2">${childRun.dag_run_id}</span>
        </div>
        <div class="child-meta">
          <span><i class="bi bi-calendar me-1"></i>${formatDateTime(childRun.execution_date)}</span>
          ${childRun.start_date ? ` | <i class="bi bi-clock me-1"></i>Started: ${formatDateTime(childRun.start_date)}` : ''}
          ${childRun.end_date ? ` | <i class="bi bi-stopwatch me-1"></i>Ended: ${formatDateTime(childRun.end_date)}` : ''}
        </div>
      </div>
    `;
  }

  // Конфигурация
  const confHtml = detail && detail.conf && Object.keys(detail.conf).length > 0 ? `
    <button class="btn btn-sm btn-outline-secondary mt-2" onclick="toggleConf('${dag.dag_id}-${dag.dag_run_id}')">
      <i class="bi bi-code-square me-1"></i> Configuration
    </button>
    <div id="conf-${dag.dag_id}-${dag.dag_run_id}" class="conf-block">
      ${JSON.stringify(detail.conf, null, 2)}
    </div>
  ` : '';

  const triggerInfo = getTriggerType(dag);

  let durationText = 'N/A';
  if (dag.start_date && dag.end_date) {
    durationText = formatDuration((new Date(dag.end_date) - new Date(dag.start_date)) / 1000);
  } else if (dag.start_date) {
    durationText = 'Running...';
  }

  dagCard.innerHTML = `
    <div class="d-flex justify-content-between align-items-start">
      <div style="flex:1; cursor:pointer;" onclick="toggleTaskContainer('${dag.dag_id}', '${dag.dag_run_id}')">
        <div class="d-flex align-items-center gap-2 flex-wrap">
          <h5 class="fw-bold mb-1">
            <i class="bi ${statusIcons[dag.state] || 'bi-question-circle'} me-2"></i>
            ${vendor} ${model}
          </h5>
          <span class="trigger-type ${triggerInfo.type}">
            <i class="bi ${triggerInfo.icon}"></i> ${triggerInfo.label}
          </span>
          <span class="badge ${getStatusBadgeClass(dag.state)}">${dag.state.toUpperCase()}</span>
          ${childRun ? '<span class="badge bg-primary">Has Child</span>' : ''}
        </div>

        <div class="dag-meta-grid">
          ${dag.version ? `<span class="meta-item"><i class="bi bi-tag"></i>Версия: ${dag.version}</span>` : ''}
          ${dag.filename ? `<span class="meta-item"><i class="bi bi-file-earmark"></i>Файл: ${dag.filename}</span>` : ''}
          <span class="meta-item"><i class="bi bi-hash"></i>${dag.dag_run_id}</span>
          <span class="meta-item"><i class="bi bi-clock"></i>Начато: ${formatDateTime(dag.start_date)}</span>
          ${dag.end_date ? `<span class="meta-item"><i class="bi bi-stopwatch"></i>Закончено: ${formatDateTime(dag.end_date)}</span>` : ''}
          <span class="meta-item"><i class="bi bi-hourglass-split"></i>Длительность: ${durationText}</span>
        </div>

        ${childInfoHtml}
      </div>

      <div class="text-end" style="display: flex; align-items: center; gap: 0.5rem;">
        <span class="badge badge-state ${getStatusBadgeClass(dag.state)}">
          ${dag.state.toUpperCase()}
        </span>
        ${dag.state === 'running' || dag.state === 'queued' ? `
          <div class="cancel-btn" onclick="event.stopPropagation(); cancelDagRun('${dag.dag_id}', '${dag.dag_run_id}', '${vendor}', '${model}')" title="Отменить выполнение dag run">
            <i class="bi bi-x"></i>
          </div>
        ` : ''}
      </div>
    </div>
  `;

  const taskContainer = document.createElement('div');
  taskContainer.className = 'task-container';
  taskContainer.id = `tasks-${dag.dag_id}-${dag.dag_run_id}`;

  // Child задачи
  let childTasksHtml = '';
  if (childRun && childRun.tasks && childRun.tasks.length > 0) {
    childTasksHtml = `
      <div class="child-tasks-section">
        <div class="child-label">
          <i class="bi bi-diagram-3"></i>
          Child Tasks (${childRun.tasks.length})
        </div>
        ${childRun.tasks.map(task => {
          const stateInfo = getTaskStateInfo(task.state);
          return `
            <div class="task-item">
              <div class="task-header" onclick="toggleTaskDetails('${task.task_id.replace(/[^a-zA-Z0-9_-]/g, '-')}')">
                <div>
                  <i class="bi ${stateInfo.icon} ${stateInfo.color} me-2"></i>
                  <strong>${task.task_id}</strong>
                  ${task.task_type ? `<span class="text-muted small ms-2">(${task.task_type})</span>` : ''}
                </div>
                <div>
                  <span class="badge ${getStatusBadgeClass(task.state)}">${task.state.toUpperCase()}</span>
                  <i class="bi bi-chevron-down ms-1"></i>
                </div>
              </div>
              <div class="task-details" id="details-${task.task_id.replace(/[^a-zA-Z0-9_-]/g, '-')}" style="display:none;">
                <div class="detail-row">
                  <span class="detail-item"><i class="bi bi-clock"></i> <strong>Duration:</strong> ${formatDuration(task.duration)}</span>
                  <span class="detail-item"><i class="bi bi-arrow-repeat"></i> <strong>Attempts:</strong> ${task.try_number || 0}</span>
                  ${task.pool ? `<span class="detail-item"><i class="bi bi-water"></i> <strong>Pool:</strong> ${task.pool}</span>` : ''}
                  ${task.queue ? `<span class="detail-item"><i class="bi bi-hdd-stack"></i> <strong>Queue:</strong> ${task.queue}</span>` : ''}
                  ${task.priority_weight ? `<span class="detail-item"><i class="bi bi-sort-numeric-up"></i> <strong>Priority:</strong> ${task.priority_weight}</span>` : ''}
                </div>
                <div class="detail-row mt-1">
                  <span class="detail-item"><i class="bi bi-play-circle"></i> <strong>Started:</strong> ${formatDateTime(task.start_date)}</span>
                  <span class="detail-item"><i class="bi bi-stop-circle"></i> <strong>Ended:</strong> ${formatDateTime(task.end_date)}</span>
                </div>
              </div>
            </div>
          `;
        }).join('')}
      </div>
    `;
  }

  taskContainer.innerHTML = `
    <div class="p-3 border-top">
      <div>
        <div class="d-flex align-items-center gap-2 mb-2">
          <i class="bi bi-list-task text-primary"></i>
          <strong>Tasks</strong>
        </div>
        <div id="tasks-list-${dag.dag_id}-${dag.dag_run_id}">
          <div class="text-center py-2">
            <div class="spinner-border spinner-border-sm text-primary"></div>
          </div>
        </div>
      </div>
      ${childTasksHtml}
      ${confHtml}
    </div>
  `;

  dagItem.appendChild(dagCard);
  dagItem.appendChild(taskContainer);
  elements.dagList.appendChild(dagItem);

  // Загружаем задачи родителя
  try {
    const tasks = await fetchTasks(dag.dag_id, dag.dag_run_id);
    const tasksList = document.getElementById(`tasks-list-${dag.dag_id}-${dag.dag_run_id}`);
    if (tasksList) {
      if (tasks && tasks.length > 0) {
        tasksList.innerHTML = tasks.map(task => {
          const stateInfo = getTaskStateInfo(task.state);
          return `
            <div class="task-item">
              <div class="task-header" onclick="toggleTaskDetails('${task.task_id.replace(/[^a-zA-Z0-9_-]/g, '-')}')">
                <div>
                  <i class="bi ${stateInfo.icon} ${stateInfo.color} me-2"></i>
                  <strong>${task.task_id}</strong>
                  ${task.task_type ? `<span class="text-muted small ms-2">(${task.task_type})</span>` : ''}
                </div>
                <div>
                  <span class="badge ${getStatusBadgeClass(task.state)}">${task.state.toUpperCase()}</span>
                  <i class="bi bi-chevron-down ms-1"></i>
                </div>
              </div>
              <div class="task-details" id="details-${task.task_id.replace(/[^a-zA-Z0-9_-]/g, '-')}" style="display:none;">
                <div class="detail-row">
                  <span class="detail-item"><i class="bi bi-clock"></i> <strong>Duration:</strong> ${formatDuration(task.duration)}</span>
                  <span class="detail-item"><i class="bi bi-arrow-repeat"></i> <strong>Attempts:</strong> ${task.try_number || 0}</span>
                  ${task.pool ? `<span class="detail-item"><i class="bi bi-water"></i> <strong>Pool:</strong> ${task.pool}</span>` : ''}
                  ${task.queue ? `<span class="detail-item"><i class="bi bi-hdd-stack"></i> <strong>Queue:</strong> ${task.queue}</span>` : ''}
                  ${task.priority_weight ? `<span class="detail-item"><i class="bi bi-sort-numeric-up"></i> <strong>Priority:</strong> ${task.priority_weight}</span>` : ''}
                </div>
                <div class="detail-row mt-1">
                  <span class="detail-item"><i class="bi bi-play-circle"></i> <strong>Started:</strong> ${formatDateTime(task.start_date)}</span>
                  <span class="detail-item"><i class="bi bi-stop-circle"></i> <strong>Ended:</strong> ${formatDateTime(task.end_date)}</span>
                </div>
              </div>
            </div>
          `;
        }).join('');
      } else {
        tasksList.innerHTML = `<div class="text-muted small">No tasks</div>`;
      }
    }
  } catch (e) {
    console.warn('Could not load tasks for', dag.dag_id, e);
  }
}

async function renderActiveDagsWithChild(dagRuns) {
  elements.dagList.innerHTML = '';

  if (!dagRuns || dagRuns.length === 0) {
    elements.dagList.innerHTML = `
      <div class="text-center py-4 text-muted">
        <i class="bi bi-info-circle"></i>
        Активных Dag Run's не найдено
      </div>
    `;
    return;
  }

  for (const dag of dagRuns) {
    await renderSingleDagCard(dag);
  }

  elements.activeDagsCount.textContent = dagRuns.length;
}

// ================================================================
// ===== ЗАГРУЗКА ACTIVE С ПАГИНАЦИЕЙ =====
// ================================================================
async function loadActiveDagsWithPagination(reset = true) {
  if (reset) {
    activeOffset = 0;
    activeHasMore = true;
  }

  activeLoading = true;

  try {
    const endpoint = `${API_CONFIG.baseUrl}dags/${choosedDagId}/dagRuns?state=running&state=queued&limit=${activeLimit}&offset=${activeOffset}`;
    const response = await fetchApi(endpoint);
    const dagRuns = response.dag_runs || [];
    const totalEntries = response.total_entries || 0;

    activeOffset += dagRuns.length;
    activeTotal = totalEntries;
    activeHasMore = activeOffset < totalEntries;

    if (reset) {
      await renderActiveDagsWithChild(dagRuns);
    } else {
      const existingIds = new Set();
      document.querySelectorAll('.dag-card').forEach(card => {
        const id = card.closest('.list-group-item')?.id;
        if (id) existingIds.add(id);
      });

      const newDags = dagRuns.filter(dag => !existingIds.has(dag.dag_id + dag.dag_run_id));
      if (newDags.length > 0) {
        for (const dag of newDags) {
          await renderSingleDagCard(dag);
        }
        elements.activeDagsCount.textContent = document.querySelectorAll('.dag-card').length;
      }
    }

    activeLoading = false;
    elements.loadingMoreIndicator.style.display = 'none';

  } catch (error) {
    console.error('Error loading active DAGs:', error);
    activeLoading = false;
    elements.loadingMoreIndicator.style.display = 'none';
    if (reset) {
      elements.dagList.innerHTML = `
        <div class="text-center py-4 text-danger">
          <i class="bi bi-exclamation-triangle"></i>
          Ошибка загрузки активных DAG-ов: ${error.message}
          <br>
          <button class="btn btn-sm btn-outline-primary mt-2" onclick="loadActiveDagsWithPagination(true)">
            <i class="bi bi-arrow-clockwise"></i> Повторить
          </button>
        </div>
      `;
    }
  }
}

// ================================================================
// ===== ФУНКЦИИ ДЛЯ ТОГГЛА =====
// ================================================================
function toggleTaskContainer(dagId, dagRunId) {
  const container = document.getElementById(`tasks-${dagId}-${dagRunId}`);
  if (!container) return;

  const isOpen = container.classList.contains('open');

  document.querySelectorAll('.task-container.open').forEach(el => {
    if (el.id !== `tasks-${dagId}-${dagRunId}`) {
      el.classList.remove('open');
    }
  });

  container.classList.toggle('open');
}

function toggleTaskDetails(detailId) {
  const el = document.getElementById(`details-${detailId}`);
  if (!el) return;
  el.style.display = el.style.display === 'none' ? 'block' : 'none';
}

function toggleConf(uniqueId) {
  const el = document.getElementById(`conf-${uniqueId}`);
  if (!el) return;
  el.classList.toggle('show');
}

// ================================================================
// ===== БЕСКОНЕЧНЫЙ СКРОЛЛ =====
// ================================================================
function setupInfiniteScroll() {
  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting && activeHasMore && !activeLoading) {
        elements.loadingMoreIndicator.style.display = 'block';
        loadActiveDagsWithPagination(false);
      }
    });
  }, {
    root: null,
    rootMargin: '100px',
    threshold: 0.1
  });

  if (elements.scrollTrigger) {
    observer.observe(elements.scrollTrigger);
  }
}

// ================================================================
// ===== ОСНОВНАЯ ФУНКЦИЯ ЗАГРУЗКИ =====
// ================================================================
async function fetchDataWithChild(dag_id) {
  try {
    showLoading(true);
    showStatsLoading(true);
    showLoadingCompleted(true);

    const allDagRuns = await fetchAllDagRuns(dag_id);
    updateStats(allDagRuns);

    await loadActiveDagsWithPagination(true);

    showLoading(false);
    showStatsLoading(false);

    await fetchAndRenderCompletedTasks(true, 0);

  } catch (error) {
    console.error('Ошибка при отправке запроса:', error);
  } finally {
    showLoading(false);
    showStatsLoading(false);
    showLoadingCompleted(false);
  }
}

// ================================================================
// ===== СТАТИСТИКА =====
// ================================================================
function updateStats(dagRuns) {
  const counts = dagRuns.reduce((acc, run) => {
    acc[run.state] = (acc[run.state] || 0) + 1;
    return acc;
  }, { running: 0, success: 0, failed: 0, queued: 0 });

  elements.runningCount.textContent = counts.running;
  elements.successCount.textContent = counts.success;
  elements.failedCount.textContent = counts.failed;
  elements.queuedCount.textContent = counts.queued;
}

function showLoading(show) {
  elements.loadingIndicator.style.display = show ? 'block' : 'none';
}

function showStatsLoading(show) {
  const display = show ? 'block' : 'none';
  elements.runningSpinner.style.display = display;
  elements.successSpinner.style.display = display;
  elements.failedSpinner.style.display = display;
  elements.queuedSpinner.style.display = display;
}

function showLoadingCompleted(show) {
  elements.CompletedLoadingIndicator.style.display = show ? 'block' : 'none';
}

// ================================================================
// ===== ЗАВЕРШЕННЫЕ ЗАДАЧИ =====
// ================================================================
async function fetchAndRenderCompletedTasks(reload = false, offset) {
  let completedTasks;
  showLoadingCompleted(true);
  if ((offset === 0) && (reload)) {
    completedTasks = await fetchCompletedTasks(offset);
    offsetCompletedTask = 0;
  } else {
    offsetCompletedTask += 30;
    completedTasks = await fetchCompletedTasks(offsetCompletedTask);
  }
  renderCompletedTasks(completedTasks, reload);
  showLoadingCompleted(false);
}

function renderCompletedTasks(CompletedDagRuns, reload = false) {
  if (reload) {
    elements.completedDagList.innerHTML = '';
  }

  if (CompletedDagRuns["total_count"] === 0) {
    elements.completedDagList.innerHTML = `
      <div class="text-center py-4 text-muted" style="position: absolute; left: 35%; top: 20%">
        <i class="bi bi-info-circle"></i>
        Завершенныx задач не найдено
      </div>
    `;
    return;
  }

  const dags = CompletedDagRuns["es_response"] || [];
  dags.forEach(({ _source }) => {
    const firmware_vendor = _source.firmware_vendor;
    const firmware_model = _source.firmware_model;
    const firmware_version = _source.firmware_version;
    const firmware_name = _source.firmware_name;
    const timestamp = formatDateTime(_source.timestamp);
    const isFailed = _source.status === 'failed';
    const isSelected = selectedDags.has(_source.file_hash);

    const dagLink = document.createElement('a');
    dagLink.href = `/tasks/${_source.file_hash}`;
    dagLink.className = 'card-link';
    const dagCard = document.createElement('div');
    dagCard.className = `completed-dag-card ${isFailed ? 'failed' : 'success'}`;
    dagCard.dataset.dagId = _source.file_hash;
    dagCard.innerHTML = `
      <input type="checkbox" class="form-check-input selection-checkbox" ${isSelected ? 'checked' : ''} onclick="event.stopPropagation(); toggleDagSelection('${_source.file_hash}', true)">
      <div class="selection-overlay"></div>
      <div class="field">
        <i class="bi bi-person-fill"></i>
        <div>
          <div class="field-label">Производитель</div>
          <span>${firmware_vendor}</span>
        </div>
      </div>
      <div class="field">
        <i class="bi bi-gear"></i>
        <div>
          <div class="field-label">Модель</div>
          <span>${firmware_model}</span>
        </div>
      </div>
      <div class="field">
        <i class="bi bi-clock-history"></i>
        <div>
          <div class="field-label">Версия</div>
          <span>${firmware_version}</span>
        </div>
      </div>
      <div class="field">
        <i class="bi bi-file-earmark-text"></i>
        <div>
          <div class="field-label">Имя файла</div>
          <span>${firmware_name}</span>
        </div>
      </div>
      <div class="field">
        <i class="bi bi-clock"></i>
        <div>
          <div class="field-label">Время разбора</div>
          <span>${timestamp}</span>
        </div>
      </div>
    `;
    dagLink.appendChild(dagCard);
    elements.completedDagList.appendChild(dagLink);

    dagCard.addEventListener('click', (e) => {
      if (!e.target.classList.contains('selection-checkbox')) {
        if (!firstCardSelected) {
          return;
        } else {
          toggleDagSelection(_source.file_hash, false, e);
        }
      }
    });
  });

  elements.completedDagsCount.textContent = CompletedDagRuns["total_count"];

  if (CompletedDagRuns["total_count"] < (offsetCompletedTask + 30)) {
    elements.loadMoreBtn.disabled = true;
  } else {
    elements.loadMoreBtn.disabled = false;
  }
}

// ================================================================
// ===== ВЫБОР КАРТОЧЕК =====
// ================================================================
function toggleSelectionMode() {
  const isSelectionMode = selectedDags.size > 0;
  if (isSelectionMode) {
    elements.selectionControls.classList.add('show');
  } else {
    elements.selectionControls.classList.remove('show');
    elements.selectAllCheckbox.checked = false;
  }
  updateSelectedCount();
}

function toggleDagSelection(dagId, isCheckboxClick = false, event = null) {
  if (event) {
    event.preventDefault();
    event.stopPropagation();
  }
  if (!firstCardSelected && !isCheckboxClick) {
    return;
  }
  if (selectedDags.has(dagId)) {
    selectedDags.delete(dagId);
  } else {
    selectedDags.add(dagId);
    if (!firstCardSelected && isCheckboxClick) {
      firstCardSelected = true;
    }
  }
  toggleSelectionMode();
  updateCardSelection(dagId);
}

function selectAllDags() {
  const allDagCards = document.querySelectorAll('.completed-dag-card');
  allDagCards.forEach(card => {
    const dagId = card.dataset.dagId;
    selectedDags.add(dagId);
    updateCardSelection(dagId);
  });
  toggleSelectionMode();
}

function clearSelection() {
  selectedDags.clear();
  firstCardSelected = false;
  const allDagCards = document.querySelectorAll('.completed-dag-card');
  allDagCards.forEach(card => {
    card.classList.remove('selected');
    const checkbox = card.querySelector('.selection-checkbox');
    if (checkbox) {
      checkbox.checked = false;
    }
  });
  toggleSelectionMode();
}

function updateCardSelection(dagId) {
  const card = document.querySelector(`.completed-dag-card[data-dag-id="${dagId}"]`);
  if (card) {
    const isSelected = selectedDags.has(dagId);
    card.classList.toggle('selected', isSelected);
    const checkbox = card.querySelector('.selection-checkbox');
    if (checkbox) {
      checkbox.checked = isSelected;
    }
  }
}

function updateSelectedCount() {
  const count = selectedDags.size;
  if (count === 0) {
    firstCardSelected = false;
  }
  elements.selectedCount.textContent = `${count} объектов выбрано`;
  elements.selectAllCheckbox.checked = count > 0 && count === document.querySelectorAll('.completed-dag-card').length;
}

async function deleteSelectedDags() {
  if (selectedDags.size === 0) return;

  if (!confirm(`Вы уверены что хотите удалить ${selectedDags.size} объектов?`)) {
    return;
  }

  try {
    const custom_dag_id = `manual__${Date.now()}`;

    const option = {
      method: 'POST',
      headers: API_CONFIG.headers,
    };

    if (!accessToken) {
      await getToken();
    }

    option.headers["Authorization"] = `Bearer ${accessToken}`;
    option["body"] = JSON.stringify({
      'dag_run_id': custom_dag_id,
      "conf": {
        "hash_ids": Array.from(selectedDags),
      }
    });

    const response = await fetch(API_CONFIG.baseUrl + `dags/cleaner/dagRuns`, option);
    if (!response.ok) {
      throw new Error(`Ошибка при запуске удаления задачи ${response.status}`);
    }
    await checkDagStatus("cleaner", custom_dag_id, `удаление ${selectedDags.size} объектов`);
    clearSelection();
    fetchAndRenderCompletedTasks(true, 0);
  } catch (error) {
    console.error("Ошибка удаления объектов", error);
    showStatus(selectedDags[1], `Ошибка удаления ${selectedDags.size} выбранных объектов`, `danger`, true);
  }
}

// ================================================================
// ===== СТАТУСЫ И УВЕДОМЛЕНИЯ =====
// ================================================================
function showStatus(actionId, message, status, isFinal = false) {
  let alertEl = alertsMap[actionId];

  if (!alertEl) {
    alertEl = document.createElement('div');
    alertEl.className = `alert alert-${status} alert-dismissible fade show m-3`;
    alertEl.innerHTML = `
      ${message}
      <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    elements.divForStatusMessages.appendChild(alertEl);
    alertsMap[actionId] = alertEl;
  }

  alertEl.className = `alert alert-${status} alert-dismissible fade show m-3`;
  alertEl.innerHTML = `
    ${message}
    <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
  `;

  if (isFinal) {
    setTimeout(() => {
      alertEl.remove();
      delete alertsMap[actionId];
    }, 5000);
  }
}

async function checkDagStatus(dag_id, dag_run_id, message_info) {
  return new Promise((resolve) => {
    async function check() {
      const option = {
        method: 'GET',
        headers: API_CONFIG.headers,
      };

      if (!accessToken) {
        await getToken();
      }

      option.headers["Authorization"] = `Bearer ${accessToken}`;

      const response = await fetch(API_CONFIG.baseUrl + `dags/${dag_id}/dagRuns/${dag_run_id}`, option);
      const data = await response.json();
      showStatus(dag_id + dag_run_id, `Cтатус Dag Run - ${data.state}, ${message_info}`, 'primary');
      if (data.state === 'success' || data.state === 'failed') {
        showStatus(dag_id + dag_run_id, `Задача выполнена ${message_info} со статусом ${data.state}`, data.state === 'success' ? 'success' : 'danger', true);
        resolve();
      } else {
        setTimeout(() => check(), 2000);
      }
    }
    check();
  });
}

// ================================================================
// ===== ПОИСК =====
// ================================================================
function getUrlParametrs() {
  const urlParams = new URLSearchParams(window.location.search);
  return {
    vendor: urlParams.get('vendor') || '',
    model: urlParams.get('model') || ''
  };
}

function checkUrlParameters() {
  const urlParams = getUrlParametrs();

  if (urlParams.vendor || urlParams.model) {
    elements.searchVendor.value = urlParams.vendor;
    elements.searchModel.value = urlParams.model;

    currentSearchParams = urlParams;

    showSearchInfo();

    if (!elements.completedTab.classList.contains('active')) {
      elements.completedTab.click();
    }
  }
}

function showSearchInfo() {
  const criteria = [];
  if (currentSearchParams.vendor) {
    criteria.push(`Производитель: "${currentSearchParams.vendor}"`);
  }
  if (currentSearchParams.model) {
    criteria.push(`Модель: "${currentSearchParams.model}"`);
  }

  elements.searchCriteria.textContent = criteria.join(', ');
  elements.searchResultsInfo.classList.remove('d-none');
}

function clearSearchInfo() {
  elements.searchVendor.value = '';
  elements.searchModel.value = '';
  currentSearchParams = { vendor: '', model: '' };
  elements.searchResultsInfo.classList.add('d-none');
  fetchAndRenderCompletedTasks(true, 0);
}

function buildSearch() {
  currentSearchParams = {
    vendor: elements.searchVendor.value.trim(),
    model: elements.searchModel.value.trim()
  };

  if (currentSearchParams.vendor || currentSearchParams.model) {
    fetchAndRenderCompletedTasks(true, 0);
    showSearchInfo();
  }
}

// ================================================================
// ===== ОТМЕНА DAG RUN =====
// ================================================================
async function cancelDagRun(dag_id, dag_run_id, dag_vendor, dag_model) {
  if (!confirm(`Вы уверены, что хотите отменить данную задачу?`)) {
    return;
  }

  let custom_dag_id = `manual__${Date.now()}`;

  try {
    document.getElementById(`${dag_id + dag_run_id}`).remove();

    const option = {
      method: 'POST',
      headers: API_CONFIG.headers,
    };

    if (!accessToken) {
      await getToken();
    }

    option.headers["Authorization"] = `Bearer ${accessToken}`;
    option["body"] = JSON.stringify({
      'dag_run_id': custom_dag_id,
      "conf": {
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        'refresh_dag': false
      }
    });

    const response = await fetch(API_CONFIG.baseUrl + `dags/manager/dagRuns`, option);
    if (!response.ok) {
      throw new Error(`Ошибка при запуске отмены задачи ${response.status}`);
    }

    await checkDagStatus("manager", custom_dag_id, `отмена ${dag_vendor} ${dag_model}`);
  } catch (error) {
    console.error(`Ошибка при отмене задачи ${error.message}`);
    fetchDataWithChild(choosedDagId);
    showStatus(custom_dag_id, `Ошибка при отмене задачи ${dag_vendor} ${dag_model}`, "danger");
  }
}

// ================================================================
// ===== ПЕРЕКЛЮЧЕНИЕ РЕЖИМОВ =====
// ================================================================
function updateCurrentMode() {
  if (elements.toggleMode.checked) {
    choosedDagId = CrawDagId;
  } else {
    choosedDagId = ArmDagId;
  }
  fetchDataWithChild(choosedDagId);
}

// ================================================================
// ===== АВТО-ОБНОВЛЕНИЕ =====
// ================================================================
function toggleAutoRefresh() {
  const isAutoRefreshEnabled = elements.autoRefreshCheckbox.checked;

  if (isAutoRefreshEnabled) {
    stopAutoRefresh();
    autoRefreshInterval = setInterval(() => fetchDataWithChild(choosedDagId), 10000);
  } else {
    stopAutoRefresh();
  }
}

function stopAutoRefresh() {
  if (autoRefreshInterval) {
    clearInterval(autoRefreshInterval);
    autoRefreshInterval = null;
  }
}

// ================================================================
// ===== ОБРАБОТЧИКИ СОБЫТИЙ =====
// ================================================================
function initEventListeners() {
  elements.loadMoreBtn.addEventListener('click', () => fetchAndRenderCompletedTasks(false, offsetCompletedTask));
  elements.refreshBtn.addEventListener('click', () => fetchDataWithChild(choosedDagId));
  elements.autoRefreshCheckbox.addEventListener('change', toggleAutoRefresh);
  elements.toggleMode.addEventListener('change', updateCurrentMode);
  elements.clearSearch.addEventListener('click', clearSearchInfo);
  elements.searchBtn.addEventListener('click', buildSearch);
  elements.selectAllCheckbox.addEventListener('change', (e) => {
    if (e.target.checked) {
      selectAllDags();
    } else {
      clearSelection();
    }
  });
  elements.deleteSelectedBtn.addEventListener('click', deleteSelectedDags);
  elements.clearSelectionBtn.addEventListener('click', clearSelection);
}

// ================================================================
// ===== ИНИЦИАЛИЗАЦИЯ =====
// ================================================================
document.addEventListener('DOMContentLoaded', function() {
  initEventListeners();
  checkUrlParameters();

  // Настраиваем бесконечный скролл
  setupInfiniteScroll();

  // Загружаем данные
  fetchDataWithChild(choosedDagId);
});

// ================================================================
// ===== ГЛОБАЛЬНЫЕ ФУНКЦИИ ДЛЯ onclick =====
// ================================================================
window.toggleConf = toggleConf;
window.toggleTaskDetails = toggleTaskDetails;
window.toggleTaskContainer = toggleTaskContainer;
window.loadActiveDagsWithPagination = loadActiveDagsWithPagination;
window.fetchAndRenderCompletedTasks = fetchAndRenderCompletedTasks;