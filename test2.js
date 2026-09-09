// ================================================================
// ===== РЕНДЕРИНГ ОДНОЙ КАРТОЧКИ (БЕЗ ЗАГРУЗКИ ЗАДАЧ) =====
// ================================================================
async function renderSingleDagCard(dag) {
  const statusClass = `status-${dag.state}`;
  const statusIcons = {
    running: 'bi-play-fill text-primary',
    success: 'bi-check-circle-fill text-success',
    failed: 'bi-exclamation-triangle-fill text-danger',
    queued: 'bi-hourglass text-warning'
  };

  // Получаем детальную информацию (conf)
  let detail = null;
  try {
    detail = await fetchDagRunDetail(dag.dag_id, dag.dag_run_id);
  } catch (e) {
    console.warn('Could not fetch detail for', dag.dag_id, e);
  }

  // Ищем Child DAG Run (только информацию, без задач)
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

  // Child информация (без задач)
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
        <!-- Child задачи будут загружены при клике -->
        <div id="child-tasks-container-${dag.dag_id}-${dag.dag_run_id}" style="display:none; margin-top: 0.5rem;">
          <div class="text-center py-2">
            <div class="spinner-border spinner-border-sm text-primary"></div>
          </div>
        </div>
      </div>
    `;
  }

  // Конфигурация
  const confHtml = detail && detail.conf && Object.keys(detail.conf).length > 0 ? `
    <button class="btn btn-sm btn-outline-secondary mt-2" onclick="event.stopPropagation(); toggleConf('${dag.dag_id}-${dag.dag_run_id}')">
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
      <div style="flex:1; cursor:pointer;" onclick="toggleDagCard('${dag.dag_id}', '${dag.dag_run_id}')">
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

  // Контейнер для задач (скрыт, открывается по клику)
  const taskContainer = document.createElement('div');
  taskContainer.className = 'task-container';
  taskContainer.id = `tasks-${dag.dag_id}-${dag.dag_run_id}`;
  
  // Содержимое контейнера - заглушка, будет загружено при клике
  taskContainer.innerHTML = `
    <div class="p-3 border-top">
      <div class="text-center py-2">
        <div class="spinner-border spinner-border-sm text-primary"></div>
        <p class="text-muted mt-2">Загрузка задач...</p>
      </div>
    </div>
  `;

  dagItem.appendChild(dagCard);
  dagItem.appendChild(taskContainer);
  elements.dagList.appendChild(dagItem);
}

// ================================================================
// ===== ФУНКЦИЯ ДЛЯ ОТКРЫТИЯ/ЗАКРЫТИЯ КАРТОЧКИ =====
// ================================================================
async function toggleDagCard(dagId, dagRunId) {
  const container = document.getElementById(`tasks-${dagId}-${dagRunId}`);
  if (!container) return;

  const isOpen = container.classList.contains('open');

  // Закрываем все открытые карточки
  document.querySelectorAll('.task-container.open').forEach(el => {
    if (el.id !== `tasks-${dagId}-${dagRunId}`) {
      el.classList.remove('open');
    }
  });

  if (isOpen) {
    container.classList.remove('open');
    return;
  }

  // Открываем
  container.classList.add('open');

  // Если задачи еще не загружены - загружаем
  if (container.dataset.loaded !== 'true') {
    await loadDagCardContent(dagId, dagRunId, container);
    container.dataset.loaded = 'true';
  }
}

// ================================================================
// ===== ЗАГРУЗКА СОДЕРЖИМОГО КАРТОЧКИ =====
// ================================================================
async function loadDagCardContent(dagId, dagRunId, container) {
  try {
    // Загружаем задачи родителя
    const tasks = await fetchTasks(dagId, dagRunId);
    
    // Загружаем Child задачи (если есть)
    const childRun = await findChildDagRuns(dagId, dagRunId);
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

    // Формируем HTML с задачами
    let tasksHtml = '';
    if (tasks && tasks.length > 0) {
      tasksHtml = tasks.map(task => {
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
      tasksHtml = '<div class="text-muted small">No tasks</div>';
    }

    // Обновляем содержимое контейнера
    container.innerHTML = `
      <div class="p-3 border-top">
        <div>
          <div class="d-flex align-items-center gap-2 mb-2">
            <i class="bi bi-list-task text-primary"></i>
            <strong>Tasks</strong>
          </div>
          ${tasksHtml}
        </div>
        ${childTasksHtml}
      </div>
    `;

  } catch (error) {
    console.error('Error loading tasks:', error);
    container.innerHTML = `
      <div class="p-3 border-top">
        <div class="text-center py-4 text-danger">
          <i class="bi bi-exclamation-triangle"></i>
          Ошибка загрузки задач: ${error.message}
        </div>
      </div>
    `;
  }
}