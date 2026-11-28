'use strict';

const $ = id => document.getElementById(id);
const preferenceKey = 'central.workspace.preferences.v1';
const preferences = loadPreferences();
let token = '';
let profiles = [];
let modelCatalog = [];
let selected = preferences.selected || '';
let view = preferences.view === 'office' ? 'office' : 'work';
let busy = false;
const projects = new Map();
for (const [repository, runIds] of Object.entries(preferences.projects || {})) {
    projects.set(repository, projectState(repository, runIds));
}

function projectState(repository, runIds = []) {
    return { repository, runIds, draft: '', mode: 'plan', roles: null, run: null, chat: [], attempt: '', logs: [], logAttempt: '', exitPlanningFor: '' };
}

function loadPreferences() {
    try {
        return JSON.parse(localStorage.getItem(preferenceKey)) || {};
    } catch {
        return {};
    }
}

function savePreferences() {
    preferences.selected = selected;
    preferences.view = view;
    preferences.projects = Object.fromEntries(
        [...projects].map(([path, workspace]) => [path, workspace.runIds]),
    );
    try {
        localStorage.setItem(preferenceKey, JSON.stringify(preferences));
    } catch {
        // Storage may be disabled. The current browser session stays usable.
    }
}

function node(tag, text = '', className = '') {
    const element = document.createElement(tag);
    element.textContent = text;
    element.className = className;
    return element;
}

function button(text, handler, className = 'btn') {
    const element = node('button', text, className);
    element.type = 'button';
    element.addEventListener('click', handler);
    return element;
}

function field(label, input) {
    const element = node('label', '', 'field');
    input.setAttribute('aria-label', label);
    element.append(node('span', label, 'sm'), input);
    return element;
}

function modal(title, ...contents) {
    $('dialog-title').textContent = title;
    $('dialog-body').replaceChildren(...contents);
    if (!$('dialog').open) $('dialog').showModal();
}

function errorMessage(message) {
    $('feedback').textContent = message;
    $('feedback').hidden = !message;
}

async function perform(operation) {
    if (busy) return;
    busy = true;
    $('send').disabled = true;
    errorMessage('');
    try {
        await operation();
    } catch (error) {
        errorMessage(error.message);
        if ($('dialog').open) {
            $('dialog-body').querySelector('.form-error')?.remove();
            const message = node('p', error.message, 'feedback form-error');
            message.setAttribute('role', 'alert');
            $('dialog-body').append(message);
        }
    } finally {
        busy = false;
        render();
    }
}

function api(path, body) {
    return request('/v1/' + path, body);
}

async function request(path, body) {
    const write = body !== undefined;
    const response = await fetch(path, {
        method: write ? 'POST' : 'GET',
        cache: 'no-store',
        headers: { Authorization: 'Bearer ' + token, ...(write ? { 'Content-Type': 'application/json' } : {}) },
        body: write ? JSON.stringify(body) : undefined,
    });
    const data = await response.json();
    if (!response.ok) {
        const error = new Error(data.error?.message || 'Không thể kết nối Central.');
        error.status = response.status;
        throw error;
    }
    return data;
}

function command(command, snapshot) {
    const suffix = snapshot ? '?run=' + encodeURIComponent(snapshot.state.run_id) : '';
    return api('commands' + suffix, {
        id: crypto.randomUUID(), expected_revision: snapshot?.revision ?? null, command,
    });
}

function runPath(run, suffix = '') {
    return 'runs/' + encodeURIComponent(run) + suffix;
}

function currentWorkspace() {
    return projects.get(selected);
}

function nameOf(path) {
    const parts = path.split(/[\\/]/).filter(Boolean);
    return parts.at(-1) || path;
}

function switchWorkspace(repository) {
    if (busy) return;
    if (!projects.has(repository)) projects.set(repository, projectState(repository));
    if (welcomeDraft) {
        projects.get(repository).draft ||= welcomeDraft;
        welcomeDraft = '';
    }
    selected = repository;
    savePreferences();
    render();
    const workspace = projects.get(repository);
    void refresh(workspace).then(() => {
        if (!busy && workspace === currentWorkspace()) render();
    }).catch(error => {
        if (!busy && workspace === currentWorkspace()) errorMessage(error.message);
    });
}

function addProject() {
    const input = node('input');
    input.placeholder = '/home/ban/Code/project';
    input.autocomplete = 'off';
    const choose = () => {
        const path = input.value.trim();
        if (!/^(?:[/]|[a-z]:[\\/]|\\\\)/i.test(path)) {
            input.setCustomValidity('Nhập đường dẫn đầy đủ đến thư mục Git trên máy.');
            input.reportValidity();
            return;
        }
        $('dialog').close();
        switchWorkspace(path);
    };
    input.addEventListener('input', () => input.setCustomValidity(''));
    input.addEventListener('keydown', event => {
        if (event.key === 'Enter') {
            event.preventDefault();
            choose();
        }
    });
    modal('Mở workspace', field('Đường dẫn thư mục Git trên máy', input),
        button('Mở workspace', choose, 'btn ink'));
    input.focus();
}

function showConnection() {
    const input = node('input');
    input.type = 'password';
    input.autocomplete = 'off';
    const load = node('input');
    load.type = 'file';
    load.addEventListener('change', async () => {
        if (load.files[0]) input.value = (await load.files[0].text()).trim();
    });
    modal('Kết nối Central', node('p', 'Nhập khoá truy cập hoặc chọn tệp daemon.token trong thư mục dữ liệu Central.'),
        field('Khoá truy cập', input), field('Chọn tệp khoá', load),
        button('Kết nối', () => perform(async () => {
            token = input.value.trim();
            try {
                await api('meta');
                await loadModelsAndProfiles();
            } catch (error) {
                token = '';
                throw error;
            }
            $('dialog').close();
            await discoverRuns();
        }), 'btn ink'));
    input.focus();
}

async function discoverRuns() {
    let cursor = null;
    do {
        const page = await api('runs?limit=20' + (cursor ? '&cursor=' + encodeURIComponent(cursor) : ''));
        const snapshots = await Promise.all(page.items.map(item => api(runPath(item.run_id))));
        for (const snapshot of snapshots) {
            const path = snapshot.state.repository;
            if (!path) continue;
            if (!projects.has(path)) projects.set(path, projectState(path));
            const workspace = projects.get(path);
            if (!workspace.runIds.includes(snapshot.state.run_id)) workspace.runIds.push(snapshot.state.run_id);
            if (!workspace.run || snapshot.state.created > workspace.run.state.created) workspace.run = snapshot;
        }
        cursor = page.next_cursor;
    } while (cursor);
    if (!selected) selected = projects.keys().next().value || '';
    savePreferences();
    await refresh(currentWorkspace());
}

async function refresh(workspace, { includeLogs = true } = {}) {
    if (!token || !workspace || workspace.newDraft) return;
    const id = workspace.run?.state.run_id || workspace.runIds.at(-1);
    if (!id) return;
    const [snapshot, chat] = await Promise.all([api(runPath(id)), api(runPath(id, '/chat?limit=100'))]);
    if (workspace.newDraft || (workspace.run && workspace.run.state.run_id !== id)) return;
    if (snapshot.state.repository !== workspace.repository) {
        workspace.runIds = workspace.runIds.filter(runId => runId !== id);
        savePreferences();
        return;
    }
    if (workspace.run?.instance_epoch === snapshot.instance_epoch && workspace.run.revision > snapshot.revision) return;
    workspace.run = snapshot;
    workspace.chat = chat.items;
    if (includeLogs && workspace.attempt) await loadLogs(workspace, id, workspace.attempt);
}

async function loadLogs(workspace, id, attempt) {
    const page = await api(runPath(id, '/logs?limit=30&attempt=' + encodeURIComponent(attempt)));
    if (workspace.attempt === attempt && workspace.run?.state.run_id === id) {
        workspace.logs = page.entries.filter(entry => entry.attempt_id === attempt);
        workspace.logAttempt = attempt;
    }
}

async function loadModelsAndProfiles() {
    const [profileList, catalog] = await Promise.all([
        api('profiles'), request('/app/models'),
    ]);
    profiles = profileList.items;
    modelCatalog = catalog.items;
}

function modelChoices() {
    const saved = profiles.filter(profile => profile.capability?.status === 'supported')
        .map(profile => ({
            key: 'profile:' + profile.profile_id, profile,
            label: profile.name + ' · ' + profile.model + ' · ' + profile.effort,
        }));
    const unsaved = modelCatalog.flatMap(({ provider, models, discovered = [] }) => {
        const available = new Set(models);
        return [...new Set([...models, ...discovered])].map(model => {
            const ready = available.has(model);
            return {
                key: 'model:' + JSON.stringify([provider, model]), provider, model,
                label: (providerLabels[provider] || provider) + ' · ' + model +
                    (ready ? ' · mặc định' : ' · Chưa bật trong Central'),
                ready,
            };
        });
    });
    return [...saved, ...unsaved];
}

function roleChoices(workspace) {
    const defaults = preferences.defaults?.[workspace?.repository];
    const rememberedWorker = typeof defaults === 'string' ? 'profile:' + defaults : defaults?.worker;
    const choices = modelChoices();
    const workerKey = workspace?.roles?.worker || rememberedWorker || choices.find(choice => choice.ready !== false)?.key;
    const leaderKey = workspace?.roles?.leader || defaults?.leader || 'worker';
    return {
        choices, workerKey, leaderKey,
        worker: choices.find(choice => choice.key === workerKey && choice.ready !== false),
        leader: choices.find(choice => choice.key === (leaderKey === 'worker' ? workerKey : leaderKey) && choice.ready !== false),
    };
}

function roleSelect(label, choices, value, inherit = false) {
    const select = node('select');
    if (inherit) select.append(option('worker', 'Dùng cùng model với Worker'));
    if (!inherit && !value) {
        const prompt = option('', 'Chọn model Worker');
        prompt.disabled = true;
        select.append(prompt);
    }
    const groups = new Map();
    for (const choice of choices) {
        const provider = choice.provider || choice.profile.provider;
        if (!groups.has(provider)) {
            const group = node('optgroup');
            group.label = providerLabels[provider] || provider;
            groups.set(provider, group);
            select.append(group);
        }
        const item = option(choice.key, choice.label);
        item.disabled = choice.ready === false;
        groups.get(provider).append(item);
    }
    if (value && !Array.from(select.options).some(item => item.value === value)) {
        const unavailable = option(value, 'Model đã chọn không còn sẵn sàng');
        unavailable.disabled = true;
        select.prepend(unavailable);
    }
    select.value = value || '';
    return { select, field: field(label, select) };
}

function chooseAgent() {
    if (!token) return showConnection();
    const workspace = currentWorkspace();
    if (!workspace) return addProject();
    const { choices, workerKey, leaderKey } = roleChoices(workspace);
    if (!choices.length) {
        modal('Chưa có agent sẵn sàng', node('p', 'Hãy bật provider có model đã kiểm tra trong Cài đặt agent.'),
            ...profiles.map(profile => node('p', profile.name + ' · ' + profile.capability?.detail, 'sm mu')),
            button('Cài đặt agent', openAgentSettings, 'btn ink'),
            button('Kiểm tra lại', () => perform(async () => {
                await loadModelsAndProfiles();
                chooseAgent();
            })));
        return;
    }
    const worker = roleSelect('Model Worker', choices, workerKey);
    const leader = roleSelect('Model Leader', choices, leaderKey, true);
    const remember = node('input');
    remember.type = 'checkbox';
    modal('Model cho Worker và Leader', worker.field, leader.field,
        node('p', 'Model chưa bật vẫn có trong danh sách. Bạn có thể kiểm tra lại trong Cài đặt agent.', 'sm mu'),
        field('Đặt làm mặc định cho project này', remember),
        button('Dùng các model này', () => {
            if (!worker.select.value || !leader.select.value ||
                worker.select.selectedOptions[0]?.disabled || leader.select.selectedOptions[0]?.disabled) {
                errorMessage('Chọn model sẵn sàng cho cả Worker và Leader.');
                return;
            }
            workspace.roles = { worker: worker.select.value, leader: leader.select.value };
            if (remember.checked) {
                preferences.defaults ??= {};
                preferences.defaults[selected] = workspace.roles;
                savePreferences();
            }
            $('dialog').close();
            render();
        }, 'btn ink'), button('Cài đặt agent', openAgentSettings));
}

async function bindableProfile(choice) {
    if (choice.profile) return choice.profile;
    const saved = profiles.find(profile => profile.provider === choice.provider &&
        profile.model === choice.model && profile.effort === 'low' &&
        profile.capability?.status === 'supported');
    if (saved) return saved;
    const result = await api('profiles', {
        name: choice.label, provider: choice.provider, model: choice.model, effort: 'low',
    });
    if (result.capability?.status !== 'supported') {
        throw new Error('Model không còn sẵn sàng: ' + choice.label);
    }
    profiles = [...profiles, result];
    return result;
}

const providerLabels = { codex: 'Codex', muse: 'Muse Code', opencode: 'opencode' };

function option(value, label = value) {
    const item = node('option', label);
    item.value = value;
    return item;
}

function openAgentSettings() {
    if (!token) return showConnection();
    void perform(loadAgentSettings);
}

async function loadAgentSettings() {
    await loadModelsAndProfiles();
    let settings = null;
    try {
        settings = (await request('/app/provider-settings')).items;
    } catch (error) {
        if (error.status !== 403) throw error;
    }
    showAgentSettings(settings);
}

function showAgentSettings(settings) {
    const contents = [];
    for (const profile of profiles) {
        const row = node('div', '', 'frow');
        const title = node('div', '', 'grow');
        title.append(node('b', profile.name), node('div', [
            providerLabels[profile.provider] || profile.provider, profile.model, profile.effort,
        ].filter(Boolean).join(' · '), 'sm mu'));
        const available = profile.capability?.status === 'supported';
        row.append(title, node('span', available ? 'Sẵn sàng' : 'Chưa sẵn sàng', available ? 'tag teal' : 'tag'),
            button('Sửa', () => showProfileEditor(settings, profile)));
        if (!available && profile.capability?.detail) title.append(node('div', profile.capability.detail, 'sm mu'));
        contents.push(row);
    }
    if (!contents.length) contents.push(node('p', 'Chưa có cấu hình agent nào.'));
    contents.push(button('Thêm cấu hình agent', () => showProfileEditor(settings), 'btn ink'));
    if (settings) {
        contents.push(button('Cấu hình provider và danh sách model', () => showProviderSettings(settings)));
    } else {
        contents.push(node('p', 'Dùng khoá chủ sở hữu Central để sửa cấu hình provider trên máy.', 'sm mu'));
    }
    modal('Cài đặt agent', ...contents);
}

function showProfileEditor(settings, existing) {
    const name = node('input');
    name.value = existing?.name || '';
    const provider = node('select');
    const choices = new Set([...Object.keys(providerLabels), ...profiles.map(profile => profile.provider)]);
    provider.append(...Array.from(choices, value => option(value, providerLabels[value] || value)));
    provider.value = existing?.provider || currentModelProvider(settings);
    const model = node('select');
    const effort = node('select');
    const syncEffort = () => {
        const advertised = modelCatalog.find(item => item.provider === provider.value)
            ?.reasoning_efforts?.[model.value];
        const levelsByProvider = {
            codex: advertised?.values ?? ['low', 'medium', 'high'],
            muse: ['none', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max', 'ultra'],
        };
        const levels = Object.hasOwn(levelsByProvider, provider.value)
            ? levelsByProvider[provider.value] : [];
        const saved = existing?.provider === provider.value && existing?.model === model.value
            ? existing.effort : '';
        if (!levels.length) {
            effort.replaceChildren(option(saved || 'low', 'Theo model · Provider quản lý'));
            effort.disabled = true;
            return;
        }
        effort.disabled = false;
        effort.replaceChildren(...Array.from(new Set(levels), level => option(level)));
        if (saved && !levels.includes(saved)) effort.append(option(saved, saved + ' · Giá trị đã lưu'));
        effort.value = saved || (levels.includes(advertised?.default) ? advertised.default
            : levels.includes('low') ? 'low' : levels[0]);
    };
    const syncModels = () => {
        const catalog = modelCatalog.find(item => item.provider === provider.value);
        const ready = new Set(catalog?.models || []);
        const ids = new Set([...ready, ...(catalog?.discovered || [])]);
        if (existing?.provider === provider.value) ids.add(existing.model);
        const items = [...ids].map(id => {
            const item = option(id, ready.has(id) ? id : id + ' · Chưa bật trong Central');
            item.disabled = !ready.has(id);
            return item;
        });
        if (!ready.size) {
            const prompt = option('', 'Provider này chưa có model dùng được');
            prompt.disabled = true;
            items.unshift(prompt);
        }
        model.replaceChildren(...items);
        model.value = existing?.provider === provider.value ? existing.model : [...ready][0] || '';
        syncEffort();
    };
    provider.addEventListener('change', syncModels);
    model.addEventListener('change', syncEffort);
    syncModels();
    modal(existing ? 'Sửa cấu hình agent' : 'Thêm cấu hình agent',
        field('Tên cấu hình', name), field('Provider', provider), field('Model', model),
        field('Mức suy luận', effort),
        node('p', 'Central kiểm tra model với provider đang chạy sau khi lưu.', 'sm mu'),
        button('Lưu cấu hình agent', () => perform(async () => {
            if (!name.value.trim()) throw new Error('Nhập tên cấu hình.');
            if (!model.value || model.selectedOptions[0]?.disabled) throw new Error('Chọn model đã bật.');
            await api('profiles', {
                profile_id: existing?.profile_id,
                name: name.value.trim(),
                provider: provider.value,
                model: model.value.trim(),
                effort: effort.value,
                config: existing?.config,
            });
            await loadAgentSettings();
        }), 'btn ink'),
        button('Quay lại', () => showAgentSettings(settings)));
    name.focus();
}

function currentModelProvider(settings) {
    const { worker } = roleChoices(currentWorkspace());
    return worker?.profile?.provider || worker?.provider ||
        settings?.find(item => item.active)?.provider || 'codex';
}

function showProviderSettings(settings, selectedProvider) {
    const provider = node('select');
    provider.append(...settings.map(item => option(item.provider, providerLabels[item.provider])));
    provider.value = selectedProvider || currentModelProvider(settings);
    if (!provider.value) provider.selectedIndex = 0;
    const current = settings.find(item => item.provider === provider.value);
    provider.addEventListener('change', () => showProviderSettings(settings, provider.value));
    const version = node('select');
    version.append(...current.supported_versions.map(value => option(value)));
    if (current.supported_versions.includes(current.cli_version)) version.value = current.cli_version;
    else {
        const unsupported = option(current.cli_version, current.cli_version + ' · Không được hỗ trợ');
        unsupported.disabled = true;
        version.prepend(unsupported);
        version.value = current.cli_version;
    }
    const models = node('textarea');
    models.rows = 4;
    models.value = current.models.join('\n');
    const enableCatalog = node('input');
    enableCatalog.type = 'checkbox';
    enableCatalog.checked = current.enable_catalog_models !== false;
    const ready = current.installed && current.valid_json;
    const description = !current.valid_json ? 'Tệp cấu hình hiện tại không đọc được.'
        : !current.installed ? 'Central chưa tìm thấy CLI của provider trên máy.'
        : current.active ? 'Provider đang có trong daemon.' : 'Provider chưa hoạt động trong daemon.';
    const save = button('Lưu cấu hình provider', () => perform(async () => {
        const result = await request('/app/provider-settings', {
            provider: current.provider,
            cli_version: version.value,
            models: models.value.split('\n').map(value => value.trim()).filter(Boolean),
            enable_catalog_models: enableCatalog.checked,
            revision: current.revision,
        });
        if (result.restart_required) {
            modal('Đã lưu cấu hình provider', node('p', 'Khởi động lại daemon để dùng phiên bản và danh sách model mới.'),
                button('Về cài đặt agent', openAgentSettings));
        }
    }), 'btn ink');
    save.disabled = !ready;
    modal('Cấu hình provider', field('Provider', provider), node('p', description, 'sm mu'),
        field('Phiên bản CLI đã cài', version), field('Bật toàn bộ model trong catalog', enableCatalog),
        field('Danh sách model thủ công, mỗi dòng một model', models),
        node('p', 'Khi tắt catalog, chỉ dùng danh sách thủ công. Phiên bản phải trùng CLI đã cài.', 'sm mu'),
        save, button('Quay lại', () => showAgentSettings(settings)));
}

function singleTaskPlan(goal, criteria) {
    return {
        version: 1, goal, tasks: [{
            id: crypto.randomUUID(), kind: 'implementation', objective: goal, inputs: [], scope: [],
            outOfScope: [], dependsOn: [], dataDependsOn: [], outputs: [],
            verify: { method: 'check', criteria }, evidence: [], profileId: null, permissions: [],
            maxRepairRounds: 2, requiresApproval: [], handover: '',
        }],
    };
}

async function ensureRun(workspace, goal, fresh = false) {
    let snapshot = workspace.run;
    if (fresh || !snapshot || snapshot.state.control === 'Stopped') {
        const receipt = await command({ CreateRun: { goal, repository: workspace.repository } });
        const id = receipt.run_id;
        if (!id) throw new Error('Central chưa trả về mã công việc.');
        workspace.runIds.push(id);
        savePreferences();
        snapshot = await api(runPath(id));
        workspace.run = snapshot;
        workspace.newDraft = false;
        workspace.chat = [];
        workspace.attempt = '';
        workspace.logs = [];
    }
    return snapshot;
}

async function bindRoles(workspace, snapshot) {
    const { worker, leader } = roleChoices(workspace);
    if (!worker || !leader) return false;
    const workerProfile = await bindableProfile(worker);
    const leaderProfile = leader.key === worker.key ? workerProfile : await bindableProfile(leader);
    const id = snapshot.state.run_id;
    await api(runPath(id, '/profile'), { role: 'worker', profile: workerProfile.profile_id });
    await api(runPath(id, '/profile'), { role: 'leader', profile: leaderProfile.profile_id });
    return true;
}

async function sendDiscussion(workspace, message, { intent = 'discuss', assume = false } = {}) {
    const snapshot = await ensureRun(workspace, message);
    if (!snapshot.state.leader_profile || !snapshot.state.worker_profile) {
        if (!await bindRoles(workspace, snapshot)) return chooseAgent();
    }
    await request('/app/' + runPath(snapshot.state.run_id, '/leader-chat'), {
        body: message, intent, ...(assume ? { assume: true } : {}),
    });
    if (workspace.draft.trim() === message) workspace.draft = '';
    await refresh(workspace);
}

// Planning mode is derived, never stored: the last turn asked to plan (or
// revise) and has not produced a proposal yet. "Quay lại thảo luận" only
// exits for that one turn, by remembering its attempt id.
function planningIntent(workspace) {
    const turn = workspace?.run?.state?.leader_discussion?.at(-1);
    if (!turn || turn.proposal) return null;
    if (turn.intent !== 'plan' && turn.intent !== 'revise') return null;
    if (workspace.exitPlanningFor && workspace.exitPlanningFor === turn.attempt_id) return null;
    return turn.intent;
}

function currentIntent(workspace) {
    return planningIntent(workspace) || 'discuss';
}

async function submitDirect(workspace, goal) {
    const previous = workspace.run?.state;
    let snapshot = await ensureRun(workspace, goal, Boolean(previous &&
        (previous.active_plan || previous.pending_plan || previous.leader_discussion?.length || previous.goal !== goal)));
    if (!await bindRoles(workspace, snapshot)) return chooseAgent();
    const id = snapshot.state.run_id;
    await api(runPath(id, '/sync'), { commit: 'HEAD' });
    snapshot = await api(runPath(id));
    await command({ ProposePlan: { plan: singleTaskPlan(goal, [goal]) } }, snapshot);
    snapshot = await api(runPath(id));
    workspace.run = snapshot;
    workspace.draft = '';
    workspace.roles = null;
    await command({ ApprovePlan: { plan_hash: snapshot.state.pending_plan } }, snapshot);
    await refresh(workspace);
}

const statusLabels = {
    Pending: 'Đang chờ', Queued: 'Đang xếp hàng', Dispatching: 'Đang giao việc',
    Running: 'Đang chạy', WaitingForApproval: 'Chờ duyệt', WaitingForInput: 'Chờ trả lời',
    Succeeded: 'Đã xác minh', Completed: 'Đã hoàn tất lượt chạy',
    AwaitingVerification: 'Chờ xác minh', AwaitingResume: 'Chờ tiếp tục',
    UnknownOutcome: 'Chưa rõ kết quả', Failed: 'Thất bại', Cancelled: 'Đã huỷ',
    Stopped: 'Đã dừng', Paused: 'Đã tạm dừng', Reconciling: 'Đang đối soát',
};
const activeStatuses = new Set(['Queued', 'Dispatching', 'Running', 'WaitingForApproval', 'WaitingForInput']);

function statusLabel(status) {
    return statusLabels[status] || status || 'Chưa bắt đầu';
}

function pendingRequests(snapshot) {
    return snapshot?.state.inbox.filter(item => item.status === 'Pending') || [];
}

function showInbox(workspace) {
    const requests = pendingRequests(workspace.run);
    modal('Cần bạn xử lý', ...(requests.length
        ? requests.map(request => button(request.preview || 'Yêu cầu từ worker', () => showRequest(workspace, request), 'rc'))
        : [node('p', 'Không có yêu cầu đang chờ.')]));
}

function showRequest(workspace, request) {
    const run = workspace.run.state.run_id;
    const key = [request.provider, request.epoch, request.thread_id, request.turn_id,
        request.item_id, request.request_id].join(':');
    const target = { key, preview_hash: request.preview_hash };
    const send = (action, values = {}) => perform(async () => {
        await api(runPath(run, '/inbox/' + action), { ...target, ...values });
        $('dialog').close();
        await refresh(workspace);
    });
    if (request.kind === 'Question') {
        const input = node(request.is_secret ? 'input' : 'textarea');
        if (request.is_secret) input.type = 'password';
        input.autocomplete = 'off';
        const isAcpForm = request.epoch.startsWith('acp-v1-') && request.item_id.startsWith('form-');
        const actions = [button('Gửi câu trả lời', () => {
            if (input.value.trim()) void send('answer', { [request.is_secret ? 'secret' : 'message']: input.value });
        }, 'btn ink')];
        if (isAcpForm) {
            actions.push(button('Từ chối', () => send('answer', { action: 'decline' })));
            actions.push(button('Hủy yêu cầu', () => send('answer', { action: 'cancel' })));
        }
        modal(isAcpForm ? `${request.provider} cần bạn trả lời` : 'Worker cần bạn trả lời',
            node('p', request.preview), field('Câu trả lời', input), ...actions);
        input.focus();
    } else {
        modal('Duyệt yêu cầu từ worker', node('pre', request.preview, 'card'),
            button('Cho phép một lần', () => send('approve'), 'btn ink'),
            button('Từ chối', () => send('decline')));
    }
}

function showPlan(workspace) {
    const snapshot = workspace?.run;
    const state = snapshot?.state;
    const plan = state?.plans.find(item => item.hash === (state.pending_plan || state.active_plan));
    if (!plan) return modal('Bảng kế hoạch', node('p', 'Chưa có kế hoạch.'));
    const list = node('ol');
    for (const task of plan.document.tasks) {
        const row = node('li', task.objective);
        for (const criterion of task.verify?.criteria || []) row.append(node('p', criterion, 'sm mu'));
        list.append(row);
    }
    const actions = state.pending_plan ? [
        button('Duyệt và chạy', () => perform(async () => {
            const current = await api(runPath(state.run_id));
            if (current.state.pending_plan !== plan.hash) throw new Error('Kế hoạch đã thay đổi. Mở lại để xem bản mới.');
            if (current.state.leader_discussion?.length) await api(runPath(state.run_id, '/sync'), { commit: 'HEAD' });
            const latest = await api(runPath(state.run_id));
            await command({ ApprovePlan: { plan_hash: plan.hash } }, latest);
            $('dialog').close();
            await refresh(workspace);
        }), 'btn ink'),
        button('Yêu cầu sửa', () => {
            const input = node('textarea');
            input.rows = 4;
            input.placeholder = 'Mô tả điều cần sửa trong kế hoạch…';
            const send = button('Gửi yêu cầu sửa', () => {
                const feedback = input.value.trim();
                if (!feedback) {
                    $('dialog-body').querySelector('.form-error')?.remove();
                    const hint = node('p', 'Hãy mô tả điều cần sửa trước khi gửi.', 'feedback form-error');
                    hint.setAttribute('role', 'alert');
                    $('dialog-body').append(hint);
                    input.focus();
                    return;
                }
                void perform(async () => {
                    try {
                        await request('/app/' + runPath(state.run_id, '/leader-chat'), { body: feedback, intent: 'revise' });
                    } catch (error) {
                        if (error.status === 409) {
                            await refresh(workspace);
                            $('dialog').close();
                            throw new Error('Kế hoạch đã thay đổi. Mở lại để xem bản mới.');
                        }
                        throw error;
                    }
                    $('dialog').close();
                    await refresh(workspace);
                });
            }, 'btn ink');
            modal('Yêu cầu sửa kế hoạch', field('Góp ý cho leader', input), send);
            input.focus();
        }),
    ] : [];
    modal('Kế hoạch', list, ...actions);
}

function showControl(workspace, action, label) {
    const run = workspace.run.state.run_id;
    modal(label, node('p', workspace.run.state.goal),
        button(label, () => perform(async () => {
            const latest = await api(runPath(run));
            await command(action, latest);
            $('dialog').close();
            await refresh(workspace);
        }), 'btn ink'));
}

function importFile(workspace, title, suffix) {
    const input = node('input');
    input.type = 'file';
    input.accept = '.json,application/json';
    modal(title, field('Tệp báo cáo JSON', input), button('Nhập báo cáo', () => perform(async () => {
        if (!input.files[0]) throw new Error('Chọn tệp báo cáo trước.');
        const report = JSON.parse(await input.files[0].text());
        await api(runPath(workspace.run.state.run_id, suffix), report);
        $('dialog').close();
        await refresh(workspace);
    }), 'btn ink'));
}

function showResult(workspace) {
    const snapshot = workspace.run;
    const state = snapshot.state;
    const attempt = state.attempts.find(item => item.attempt_id === workspace.attempt);
    const task = state.tasks.find(item => item.task_id === attempt?.task_id);
    const artifacts = state.artifacts.filter(item => !attempt || item.attempt_id === attempt.attempt_id);
    const body = [node('p', task ? statusLabel(task.status) : state.goal, 'tag')];
    for (const item of artifacts) {
        body.push(button(item.path, () => perform(async () => {
            const response = await fetch('/v1/' + runPath(state.run_id, '/artifacts/' + encodeURIComponent(item.id) + '/content'), {
                headers: { Authorization: 'Bearer ' + token }, cache: 'no-store',
            });
            if (!response.ok) throw new Error('Không đọc được tệp kết quả.');
            modal(item.path, node('pre', await response.text(), 'card'),
                button('Quay lại kết quả', () => showResult(workspace)));
        }), 'frow'));
    }
    if (!artifacts.length) body.push(node('p', 'Chưa có tệp kết quả được ghi nhận.', 'sm mu'));
    if (attempt?.status === 'Completed' && task?.status !== 'Succeeded' && task?.status !== 'AwaitingVerification') {
        body.push(button('Nhập kết quả worker', () => importFile(workspace, 'Nhập kết quả worker', '/attempts/' + encodeURIComponent(attempt.attempt_id) + '/results')));
    }
    if (task?.status === 'AwaitingVerification') {
        body.push(button('Nhập báo cáo xác minh', () => importFile(workspace, 'Nhập báo cáo xác minh', '/verdicts')));
    }
    const promotion = [...snapshot.events].reverse().find(event => event.kind === 'IntegrationPromoted');
    if (promotion) {
        body.push(node('p', 'Central đã ghi nhận tích hợp thay đổi trong công việc này.', 'tag teal'));
    } else if (task?.status === 'Succeeded') {
        body.push(node('p', 'Đã xác minh. Việc đưa thay đổi vào project cần bước tích hợp Git đã được phê duyệt.', 'sm mu'));
    }
    modal('Kết quả', ...body);
}

function requestLeaderReview(workspace, taskId) {
    const input = node('textarea');
    input.rows = 3;
    modal('Yêu cầu leader xem lại', field('Bạn muốn leader kiểm tra điều gì?', input),
        button('Gửi yêu cầu xem lại', () => perform(async () => {
            if (!input.value.trim()) throw new Error('Nhập nội dung cần kiểm tra.');
            await api(runPath(workspace.run.state.run_id, '/leader/review'), { task: taskId, objective: input.value.trim() });
            $('dialog').close();
            await refresh(workspace);
        }), 'btn ink'));
}

function renderTabs() {
    const tabs = $('tabs');
    const focused = tabs.contains(document.activeElement);
    tabs.replaceChildren(...Array.from(projects.keys(), path => {
        const tab = button(nameOf(path), () => switchWorkspace(path), 'tab' + (path === selected ? ' on' : ''));
        tab.title = path;
        if (path === selected) tab.setAttribute('aria-current', 'page');
        return tab;
    }));
    if (focused) tabs.querySelector('[aria-current]')?.focus({ preventScroll: true });
}

const chatTime = new Intl.DateTimeFormat('vi-VN', { hour: '2-digit', minute: '2-digit', hour12: false });

function messageTime(timestamp) {
    if (!Number.isFinite(timestamp) || timestamp <= 0) return null;
    const date = new Date(timestamp);
    const time = node('time', chatTime.format(date), 'message-time');
    time.dateTime = date.toISOString();
    return time;
}

let purifyHookInstalled = false;

function installPurifyLinkHook() {
    if (purifyHookInstalled || !window.DOMPurify?.addHook) return;
    purifyHookInstalled = true;
    window.DOMPurify.addHook('afterSanitizeAttributes', element => {
        if (element.tagName !== 'A') return;
        const href = element.getAttribute('href');
        let allowed = false;
        if (href) {
            try {
                const url = new URL(href);
                allowed = url.protocol === 'http:' || url.protocol === 'https:';
            } catch {
                allowed = false;
            }
        }
        if (!allowed) {
            element.removeAttribute('href');
            return;
        }
        element.setAttribute('target', '_blank');
        element.setAttribute('rel', 'noopener noreferrer');
    });
}

function renderLeaderMarkdown(body, text) {
    installPurifyLinkHook();
    const html = window.DOMPurify.sanitize(window.marked.parse(text), {
        ALLOWED_TAGS: [
            'a', 'b', 'blockquote', 'br', 'code', 'del', 'em',
            'h1', 'h2', 'h3', 'h4', 'hr', 'i', 'li', 'ol', 'p', 'pre',
            'strong', 'table', 'tbody', 'td', 'th', 'thead', 'tr', 'ul',
        ],
        FORBID_TAGS: ['img', 'style', 'script', 'iframe'],
        ALLOWED_ATTR: ['href', 'title'],
    });
    body.classList.add('message-body-markdown');
    body.innerHTML = html;
}

function renderMessage(message, workspace) {
    const isNotice = message.role === 'notice';
    const person = message.role === 'user' ? 'B' : 'L';
    const item = node('article', '', 'chat-message chat-message-' + message.role);
    const card = node('div', '', 'message-card');
    const meta = node('div', '', 'message-meta');
    meta.append(node('strong', message.author, 'message-author'));
    const time = messageTime(message.at);
    if (time) meta.append(time);
    const body = node('div', '', 'message-body');
    let renderedMarkdown = false;
    if (message.role === 'leader' && window.marked?.parse && window.DOMPurify?.sanitize) {
        try {
            renderLeaderMarkdown(body, message.body);
            renderedMarkdown = true;
        } catch {
            renderedMarkdown = false;
        }
    }
    if (!renderedMarkdown) {
        body.classList.remove('message-body-markdown');
        body.replaceChildren();
        for (const paragraph of message.body.split(/\r?\n[ \t]*\r?\n+/)) {
            body.append(node('p', paragraph));
        }
    }
    card.append(meta, body);

    if (message.plan) {
        const attachment = button('', () => showPlan(workspace), 'chat-plan');
        attachment.setAttribute('aria-label', 'Xem chi tiết kế hoạch leader đề xuất');
        const marker = node('span', '↗', 'chat-plan-marker');
        marker.setAttribute('aria-hidden', 'true');
        const tasks = message.plan.tasks || [];
        const text = node('span', '', 'chat-plan-text');
        text.append(node('strong', message.plan.goal || 'Kế hoạch Leader đề xuất'));
        text.append(node('span', tasks.length + ' công việc', 'chat-plan-summary'));
        const detail = node('span', 'Xem chi tiết', 'chat-plan-detail');
        detail.setAttribute('aria-hidden', 'true');
        attachment.append(marker, text, detail);
        card.append(attachment);
    }
    if (!isNotice) {
        const avatar = node('span', person, 'chat-avatar chat-avatar-' + message.role);
        avatar.setAttribute('aria-hidden', 'true');
        item.append(avatar);
    }
    item.append(card);
    return item;
}

function renderChat(workspace) {
    const snapshot = workspace?.run;
    const state = snapshot?.state;
    const chat = $('chat');
    const { leader } = roleChoices(workspace);
    const bound = state?.leader_profile
        ? profiles.find(profile => profile.profile_id === state.leader_profile)
        : leader?.profile || leader;
    const model = bound?.model ? (providerLabels[bound.provider] || bound.provider) + ' · ' + bound.model : '';
    $('chat-model').textContent = model;
    $('chat-model').hidden = !model;
    $('chat-workspace').textContent = workspace?.repository || '';
    $('chat-workspace').hidden = !workspace;

    const messages = [];
    const discussion = state?.leader_discussion || [];
    const started = new Map((state?.attempts || []).map(item => [item.attempt_id, item.created_at]));
    const finished = new Map((snapshot?.events || [])
        .filter(event => event.kind === 'LeaderReviewCompleted')
        .map(event => [event.payload?.attempt_id, event.at]));
    if (state && !discussion.length) messages.push({ role: 'user', author: 'Bạn', body: state.goal });
    for (const message of workspace?.chat || []) {
        const fromOwner = message.author === 'owner';
        messages.push({
            role: fromOwner ? 'user' : 'notice',
            author: fromOwner ? 'Bạn' : 'Ghi chú',
            body: message.body, at: message.at,
        });
    }
    for (const turn of discussion) {
        messages.push({ role: 'user', author: 'Bạn', body: turn.user, at: started.get(turn.attempt_id) });
        if (turn.reply) {
            const plan = turn.proposal && [state.pending_plan, state.active_plan].includes(turn.proposal)
                ? state.plans.find(plan => plan.hash === turn.proposal)?.document : null;
            messages.push({ role: 'leader', author: 'Leader', body: turn.reply, plan, at: finished.get(turn.attempt_id) });
        }
        if (turn.error || ['Failed', 'Cancelled', 'UnknownOutcome'].includes(turn.status)) {
            messages.push({ role: 'notice', author: 'Central', body: turn.error || statusLabel(turn.status) });
        }
    }

    const key = state?.run_id || selected;
    const signature = JSON.stringify(messages);
    if (chat.dataset.signature === signature && chat.dataset.workspace === key) return;
    const shouldScroll = chat.dataset.workspace !== key || chat.scrollHeight - chat.scrollTop - chat.clientHeight < 82;
    chat.dataset.signature = signature;
    chat.dataset.workspace = key;
    if (!messages.length) {
        const empty = node('div', '', 'chat-empty');
        empty.append(
            node('h2', selected ? 'Bắt đầu trao đổi' : 'Chọn workspace để bắt đầu'),
            node('p', selected ? 'Mô tả ý tưởng hoặc câu hỏi của bạn. Leader sẽ cùng bạn làm rõ trước khi đề xuất kế hoạch.' : 'Mở một project để trò chuyện với Leader.'),
        );
        chat.replaceChildren(empty);
    } else {
        chat.replaceChildren(...messages.map(message => renderMessage(message, workspace)));
        renderReplyPreview(workspace);
        if (shouldScroll) chat.scrollTop = chat.scrollHeight;
    }
}

function renderReplyPreview(workspace) {
    const chat = $('chat');
    let item = chat.querySelector('.chat-message-preview');
    const text = workspace?.replyPreview || '';
    if (!text) {
        item?.remove();
        return;
    }
    const nearBottom = chat.scrollHeight - chat.scrollTop - chat.clientHeight < 82;
    if (!item) {
        item = renderMessage({ role: 'leader', author: 'Leader · Đang trả lời', body: '' }, workspace);
        item.classList.add('chat-message-preview');
        item.setAttribute('aria-live', 'off');
        chat.append(item);
    }
    item.querySelector('.message-body').textContent = text;
    if (nearBottom) chat.scrollTop = chat.scrollHeight;
}

let replyStream;

function followReply(workspace) {
    const state = workspace?.run?.state;
    const turn = state?.leader_discussion?.at(-1);
    const active = token && state?.control === 'Running' && turn && activeStatuses.has(turn.status);
    if (replyStream?.workspace === workspace && active && replyStream.attempt === turn.attempt_id) return;
    if (replyStream) {
        replyStream.controller.abort();
        replyStream.workspace.replyPreview = '';
        replyStream = null;
        renderReplyPreview(workspace);
    }
    if (!active) return;
    const stream = { workspace, attempt: turn.attempt_id, controller: new AbortController() };
    replyStream = stream;
    void readReplyStream(stream, state.run_id);
}

async function readReplyStream(stream, runId) {
    let completed = false;
    try {
        const response = await fetch('/app/' + runPath(runId, '/leader-preview/' + encodeURIComponent(stream.attempt)), {
            cache: 'no-store',
            headers: { Authorization: 'Bearer ' + token, Accept: 'text/event-stream' },
            signal: stream.controller.signal,
        });
        if (!response.ok || !response.body) return;
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';
        try {
            while (replyStream === stream) {
                const { value, done } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });
                let end;
                while ((end = buffer.indexOf('\n\n')) !== -1) {
                    const frame = buffer.slice(0, end);
                    buffer = buffer.slice(end + 2);
                    if (replyStream !== stream) return;
                    if (frame.startsWith('event: done\n')) {
                        completed = true;
                        void pollWorkspace();
                        return;
                    }
                    if (!frame.startsWith('event: preview\n')) continue;
                    const data = frame.split('\n').find(line => line.startsWith('data: '));
                    if (!data) continue;
                    stream.workspace.replyPreview = JSON.parse(data.slice(6)).text;
                    if (currentWorkspace() === stream.workspace) renderReplyPreview(stream.workspace);
                }
            }
        } finally {
            await reader.cancel().catch(() => {});
        }
    } catch {
        // Durable polling still delivers the completed answer. The next
        // refresh can reconnect to the preview after a transient failure.
    } finally {
        if (!completed && replyStream === stream) replyStream = null;
    }
}

function selectAttempt(workspace, attempt) {
    workspace.attempt = attempt.attempt_id;
    workspace.logs = [];
    render();
    void perform(() => loadLogs(workspace, workspace.run.state.run_id, attempt.attempt_id));
}

function renderWorkers(workspace) {
    const state = workspace?.run?.state;
    const discussion = new Set(state?.leader_discussion?.map(turn => turn.attempt_id) || []);
    const attempts = (state?.attempts || []).filter(attempt => !discussion.has(attempt.attempt_id))
        .sort((left, right) => right.created_at - left.created_at);
    const active = attempts.filter(attempt => activeStatuses.has(attempt.status));
    $('worker-count').textContent = active.length ? active.length + ' đang làm' : 'Chưa có worker đang chạy';
    const rows = attempts.map((attempt, index) => {
        const task = state.tasks.find(item => item.task_id === attempt.task_id);
        const profile = profiles.find(item => item.profile_id === (attempt.profile_id || state.worker_profile));
        const heading = node('div', '', 'row g8');
        const title = 'Lượt ' + (attempts.length - index) + (profile ? ' · ' + profile.name : '');
        heading.append(node('b', title, 'grow'), node('span', statusLabel(attempt.status), 'tag'));
        const row = button('', () => selectAttempt(workspace, attempt), 'w' + (workspace.attempt === attempt.attempt_id ? ' sel' : ''));
        row.append(heading, node('span', task?.objective || state.goal, 'sm mu'));
        return row;
    });
    $('workers').replaceChildren(...(rows.length ? rows : [node('p', 'Các lượt làm việc sẽ xuất hiện ở đây.', 'card sm mu')]));
    $('office-workers').replaceChildren(...attempts.slice(0, 3).map((attempt, index) =>
        button('Lượt ' + (attempts.length - index) + ' · ' + statusLabel(attempt.status), () => {
            selectAttempt(workspace, attempt);
            const task = state.tasks.find(item => item.task_id === attempt.task_id);
            modal('Lượt làm việc', node('p', task?.objective || state.goal), node('p', statusLabel(attempt.status), 'tag'),
                button('Xem kết quả', () => showResult(workspace)));
        }, 'pill')));
    const attempt = attempts.find(item => item.attempt_id === workspace?.attempt);
    const task = state?.tasks.find(item => item.task_id === attempt?.task_id);
    const detail = $('detail');
    const entries = [];
    if (state?.pending_plan) entries.push(button('Xem kế hoạch', () => showPlan(workspace)));
    if (state?.run_id && workspace.runIds.length > 1) {
        const select = node('select');
        workspace.runIds.forEach((id, index) => {
            const option = node('option', 'Công việc ' + (index + 1));
            option.value = id;
            select.append(option);
        });
        select.value = state.run_id;
        select.addEventListener('change', () => perform(async () => {
            const snapshot = await api(runPath(select.value));
            workspace.attempt = '';
            workspace.logs = [];
            workspace.run = snapshot;
            await refresh(workspace);
        }));
        entries.push(field('Lịch sử công việc', select));
    }
    if (attempt) {
        entries.push(node('h3', 'Nhật ký lượt làm việc', 'lab'));
        const logs = node('div', '', 'card log');
        logs.append(...(workspace.logs.length
            ? workspace.logs.map(entry => node('p', entry.detail || statusLabel(entry.outcome)))
            : [node('p', 'Chưa có nhật ký được ghi nhận.')]));
        entries.push(logs, button('Xem kết quả', () => showResult(workspace)));
        if (task && state.active_plan && state.leader_profile && state.control === 'Running') {
            entries.push(button('Yêu cầu leader xem lại', () => requestLeaderReview(workspace, task.task_id)));
        }
    }
    detail.replaceChildren(...entries);
}

function actionFor(workspace) {
    const snapshot = workspace?.run;
    if (!token) return ['Kết nối Central', showConnection];
    if (!selected) return ['Chọn thư mục project', addProject];
    const count = pendingRequests(snapshot).length;
    if (count) return [count + ' yêu cầu đang chờ', () => showInbox(workspace)];
    if (snapshot?.state.control === 'AwaitingResume' || snapshot?.state.control === 'Paused') {
        return ['Chờ bạn tiếp tục', () => showControl(workspace, 'Resume', 'Tiếp tục công việc')];
    }
    if (snapshot?.state.pending_plan) return ['Có kế hoạch chờ duyệt', () => showPlan(workspace)];
    const { worker, leader } = roleChoices(workspace);
    if ((!worker || !leader) && !snapshot?.state.active_plan) return ['Chọn model sẵn sàng', chooseAgent];
    return ['Không có gì cần bạn', null];
}

function render({ keepWorkerFocus = false } = {}) {
    const workspace = currentWorkspace();
    const snapshot = workspace?.run;
    const state = snapshot?.state;
    renderTabs();
    $('work').hidden = view !== 'work';
    $('office').hidden = view !== 'office';
    for (const mode of document.querySelectorAll('[data-view]')) {
        const on = mode.dataset.view === view;
        mode.classList.toggle('on', on);
        mode.setAttribute('aria-pressed', String(on));
    }
    $('project-title').textContent = selected ? nameOf(selected) : 'Chưa có project';
    $('project-path').textContent = selected;
    const active = state?.attempts.filter(attempt => activeStatuses.has(attempt.status)) || [];
    $('activity').textContent = active.length ? active.length + ' lượt đang làm việc'
        : state?.tasks.some(task => task.status === 'AwaitingVerification') ? 'Chờ xác minh'
        : state ? statusLabel(state.control === 'Running' ? null : state.control) : 'Chưa giao việc';
    $('progress').textContent = state?.goal || '';
    const [attention, handler] = actionFor(workspace);
    $('attention').textContent = attention;
    $('need').classList.toggle('need', Boolean(handler));
    $('action').hidden = !handler;
    $('action').onclick = handler;
    $('connection').textContent = token ? 'Đã kết nối' : 'Chưa kết nối';
    $('connect').textContent = token ? 'Đổi kết nối' : 'Kết nối';
    renderChat(workspace);
    if (!keepWorkerFocus) renderWorkers(workspace);
    const draft = workspace?.draft ?? welcomeDraft;
    if ($('message').value !== draft) $('message').value = draft;
    const inRun = Boolean(state?.active_plan || state?.pending_plan || state?.leader_discussion?.length);
    const waiting = state?.control === 'Running' && state.leader_discussion?.some(turn =>
        activeStatuses.has(turn.status) || turn.status === 'Reconciling');
    const unresolved = state?.control === 'Running' && state.leader_discussion?.some(turn => turn.status === 'UnknownOutcome');
    const latestTurn = state?.leader_discussion?.at(-1);
    const waitingText = {
        Queued: 'Đã gửi tin nhắn. Lượt của leader đang chờ bắt đầu.',
        Dispatching: 'Central đang mở phiên cho leader.',
        Running: 'Leader đang soạn phản hồi. Tin nhắn sẽ tự hiện khi có kết quả.',
        WaitingForApproval: 'Leader cần bạn duyệt một yêu cầu để tiếp tục.',
        WaitingForInput: 'Leader cần bạn trả lời một câu hỏi để tiếp tục.',
        Reconciling: 'Central đang kiểm tra trạng thái lượt của leader.',
    };
    const status = waiting ? waitingText[latestTurn?.status] : unresolved
        ? 'Central chưa xác nhận được kết quả lượt trước.' : '';
    if ($('leader-status').textContent !== (status || '')) $('leader-status').textContent = status || '';
    $('leader-status').hidden = !status;
    const planIntent = planningIntent(workspace);
    const leaderRunning = Boolean(state?.leader_discussion?.some(turn =>
        activeStatuses.has(turn.status) || turn.status === 'Reconciling'));
    const planStatus = planIntent ? 'Đang lập kế hoạch: leader có thể hỏi các câu cần thiết' : '';
    if ($('plan-status').textContent !== planStatus) $('plan-status').textContent = planStatus;
    $('plan-status').hidden = !planStatus;
    $('plan').hidden = !state || !state.leader_discussion?.length || Boolean(state.active_plan)
        || leaderRunning || Boolean(planIntent);
    $('plan').disabled = busy || Boolean(unresolved || state?.control === 'Paused' || state?.control === 'AwaitingResume');
    $('plan-assume').hidden = !planIntent;
    $('plan-assume').disabled = busy || leaderRunning || Boolean(unresolved);
    $('plan-back').hidden = !planIntent;
    $('plan-back').disabled = busy;
    $('refresh-chat').hidden = !token || !state;
    $('refresh-chat').disabled = busy;
    const mode = workspace?.mode || 'plan';
    $('run-mode').hidden = inRun;
    $('agent').hidden = inRun;
    const { worker, leader } = roleChoices(workspace);
    $('agent').textContent = 'Model Worker / Leader';
    $('agent').title = worker && leader ? 'Worker: ' + worker.label + ' · Leader: ' + leader.label : 'Chọn model';
    for (const control of document.querySelectorAll('[data-mode]')) {
        const on = control.dataset.mode === mode;
        control.classList.toggle('on', on);
        control.setAttribute('aria-pressed', String(on));
    }
    $('message').placeholder = inRun ? 'Nhắn cho leader…' : 'Bạn muốn trao đổi điều gì với leader?';
    $('send').textContent = waiting ? 'Đang chờ leader' : !inRun && mode === 'direct' ? 'Chạy ngay' : 'Gửi';
    $('send').disabled = busy || Boolean(waiting || unresolved || state?.control === 'Paused' || state?.control === 'AwaitingResume');
    $('stop').hidden = !state || state.control === 'Stopped';
    $('new-work').hidden = !inRun;
    followReply(workspace);
}

$('connect').addEventListener('click', showConnection);
$('agent-settings').addEventListener('click', openAgentSettings);
$('add-project').addEventListener('click', addProject);
$('agent').addEventListener('click', chooseAgent);
$('board').addEventListener('click', () => showPlan(currentWorkspace()));
$('office-leader').addEventListener('click', () => {
    view = 'work';
    savePreferences();
    render();
    $('message').focus();
});
$('plan').addEventListener('click', () => {
    const workspace = currentWorkspace();
    if (!workspace) return addProject();
    if (!token) return showConnection();
    if ($('plan').disabled) return;
    const body = workspace.draft.trim() || 'Lập kế hoạch từ cuộc trao đổi';
    void perform(() => sendDiscussion(workspace, body, { intent: 'plan' }));
});
$('plan-assume').addEventListener('click', () => {
    const workspace = currentWorkspace();
    if (!workspace) return addProject();
    if (!token) return showConnection();
    if ($('plan-assume').disabled) return;
    const body = workspace.draft.trim() || 'Lập kế hoạch từ cuộc trao đổi';
    void perform(() => sendDiscussion(workspace, body, { intent: planningIntent(workspace) || 'plan', assume: true }));
});
$('plan-back').addEventListener('click', () => {
    const workspace = currentWorkspace();
    const turn = workspace?.run?.state?.leader_discussion?.at(-1);
    if (turn) workspace.exitPlanningFor = turn.attempt_id;
    render();
    $('message').focus();
});
$('stop').addEventListener('click', () => showControl(currentWorkspace(), 'Stop', 'Dừng công việc'));
$('new-work').addEventListener('click', () => {
    const workspace = currentWorkspace();
    workspace.run = null;
    workspace.newDraft = true;
    workspace.attempt = '';
    workspace.chat = [];
    workspace.logs = [];
    workspace.replyPreview = '';
    workspace.draft = '';
    workspace.mode = 'plan';
    workspace.exitPlanningFor = '';
    render();
    $('message').focus();
});
for (const mode of document.querySelectorAll('[data-view]')) {
    mode.addEventListener('click', () => {
        view = mode.dataset.view;
        savePreferences();
        render();
    });
}
for (const mode of document.querySelectorAll('[data-mode]')) {
    mode.addEventListener('click', () => {
        if (!currentWorkspace()) return addProject();
        currentWorkspace().mode = mode.dataset.mode;
        render();
    });
}
let welcomeDraft = '';
$('message').addEventListener('input', event => {
    const workspace = currentWorkspace();
    if (workspace) workspace.draft = event.target.value;
    else welcomeDraft = event.target.value;
});
$('composer').addEventListener('submit', event => {
    event.preventDefault();
    const workspace = currentWorkspace();
    if (!workspace) return addProject();
    if (!token) return showConnection();
    const body = workspace.draft.trim();
    if (!body) return;
    if ($('send').disabled) return;
    const state = workspace.run?.state;
    const isConversation = Boolean(state?.leader_discussion?.length || state?.active_plan || state?.pending_plan);
    const { worker, leader } = roleChoices(workspace);
    if ((!state?.leader_profile || !state?.worker_profile) && (!worker || !leader)) {
        chooseAgent();
    } else if (!isConversation && workspace.mode === 'direct') {
        void perform(() => submitDirect(workspace, body));
    } else {
        void perform(() => sendDiscussion(workspace, body, { intent: currentIntent(workspace) }));
    }
});

let polling = false;
async function pollWorkspace() {
    const workspace = currentWorkspace();
    if (!token || !workspace || busy || polling || document.hidden) return;
    const workerFocused = () => $('detail').contains(document.activeElement) ||
        $('workers').contains(document.activeElement);
    polling = true;
    try {
        await refresh(workspace, { includeLogs: !workerFocused() });
        if (!busy && workspace === currentWorkspace()) render({ keepWorkerFocus: workerFocused() });
    } catch (error) {
        if (!busy) errorMessage(error.message);
    } finally {
        polling = false;
    }
}

setInterval(pollWorkspace, 2500);
$('refresh-chat').addEventListener('click', pollWorkspace);
window.addEventListener('focus', pollWorkspace);
document.addEventListener('visibilitychange', pollWorkspace);
$('dialog').addEventListener('close', pollWorkspace);

render();
