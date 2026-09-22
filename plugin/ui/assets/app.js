(function (global, factory) {
  'use strict';
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else api.start(global);
})(typeof window === 'object' ? window : null, function () {
  'use strict';
  const DEFAULT_CONFIG = Object.freeze({ force_model: '', enabled: false, auto_harvest: true, business_use_front: false, allow_without_ticket: true, dynamic_proxy_url: '', harvest_dial_proxy_url: '', harvest_dial_proxy_mode: 'direct', harvest_dial_proxy_id: 0, observe_exit_ip: false, ttl_minutes: 60,
    refresh_before_minutes: 10, max_attempts: 8, attempt_interval_seconds: 10, cooldown_seconds: 300 });
  const NUMBERS = Object.freeze({ ttl_minutes: [1, 60, '票据有效期'], refresh_before_minutes: [0, 59, '提前续期'],
    max_attempts: [1, 32, '每轮最多尝试'], attempt_interval_seconds: [1, 300, '尝试间隔'], cooldown_seconds: [30, 3600, '失败后冷却'] });
  const STATES = Object.freeze({ manual_idle:['等待手动查找',''], disabled: ['已关闭', ''], waiting_host: ['等待宿主', 'warning'],
    waiting_account: ['等待账号', 'warning'], queued: ['等待获取', ''], harvesting: ['正在获取', ''],
    checking_proxy: ['正在检查代理', 'warning'], ready: ['可用', 'success'], renewing: ['正在续期', ''], cooldown: ['冷却中', 'warning'],
    expired: ['已过期', 'warning'], error: ['获取失败', 'error'] });
  const MODEL_PATTERN = /^gpt-[A-Za-z0-9][A-Za-z0-9._-]{0,94}$/;
  const ERRORS = Object.freeze({ checking_business_proxy:'正在检查账号业务出口…',checking_dynamic_proxy:'正在检查前置及动态代理…',business_connectivity_failed:'账号业务出口检测未通过，本次查找已停止。请检查账号代理；检测服务不可达也可能造成失败。',dynamic_connectivity_failed:'本次动态出口检测未通过；还有尝试次数时会自动换出口，次数用完后停止。', managed_proxy_unavailable:'选中的前置代理不可用，请检查 IP 管理或宿主适配',  proxy_auth_failed:'代理用户名或密码验证失败', front_proxy_failed:'前置代理连接或 CONNECT 被拒绝', transport_timeout:'代理连接或请求超时', transport_tls_failed:'TLS 连接或证书验证失败', attempts_exhausted: '本轮尝试已用完', identity_unavailable: '暂时无法取得账号授权或业务代理',
    invalid_dynamic_proxy: '动态代理配置无效', harvest_failed: '动态代理获取票据未成功', unexpected_state_length: '票据长度与所选套餐不符',
    identity_changed: '账号授权信息发生变化', fixed_proxy_validation_failed: '票据未通过原业务代理验证',
    ticket_persistence_failed: '票据保存失败', upstream_unauthorized: '已收到上游响应，账号授权被拒绝（401）。请检查或重新授权此账号。', upstream_forbidden: '上游拒绝访问（403）',
    upstream_rate_limited: '上游限流（429）', upstream_rejected: '上游拒绝请求', model_mismatch: '返回模型不匹配，正在重新获取票据',
    state_312: '收到 312 状态，正在重新获取票据', model_mismatch_persistence_failed: '返回模型不匹配，票据失效记录保存失败',
    state_312_persistence_failed: '收到 312 状态，票据失效记录保存失败' });
  const MESSAGES = Object.freeze({ 'STATE disabled; requests use the account business proxy': 'STATE 已关闭，请求使用账号原有业务代理。',
    'STATE active only for explicitly enabled account/model pairs': 'STATE 仅对手动开启的账号与模型生效。',
    'waiting for host services': '正在等待宿主服务初始化。' });
  function accountID(value) {
    if (typeof value !== 'number' && typeof value !== 'string') return null;
    if (!/^[1-9]\d*$/.test(String(value).trim())) return null;
    const number = Number(value);
    return Number.isSafeInteger(number) && number > 0 ? number : null;
  }
  function normalizeConfig(input) {
    const source = input && typeof input === 'object' && !Array.isArray(input) ? input : {};
    const config = Object.assign({}, DEFAULT_CONFIG);
    Object.keys(DEFAULT_CONFIG).forEach(function (key) {
      if (source[key] !== undefined) config[key] = source[key];
    });
    if (!source.harvest_dial_proxy_mode) config.harvest_dial_proxy_mode = config.harvest_dial_proxy_url ? 'manual' : 'direct';
    config.accounts = Array.isArray(source.accounts) ? source.accounts.map(function (account) {
      return { account_id: account.account_id, enabled: account.enabled === true,
        plan: account.plan || 'pro', models: Array.isArray(account.models) ? account.models.slice() : ['gpt-6-astra'] };
    }) : [];
    return config;
  }
  function validateConfig(config) {
    const mode = config.harvest_dial_proxy_mode || (config.harvest_dial_proxy_url ? 'manual' : 'direct');
    if (!['direct', 'manual', 'managed'].includes(mode)) throw new Error('请选择前置代理方式。');
    if (mode === 'manual' && !config.harvest_dial_proxy_url) throw new Error('请填写前置代理地址。');
    if (mode === 'managed' && accountID(config.harvest_dial_proxy_id) === null) throw new Error('请选择 IP 管理中的代理。');
    if (typeof config.allow_without_ticket !== 'boolean') throw new Error('放行开关格式不正确。');
    if (typeof config.business_use_front !== 'boolean') throw new Error('业务前置代理开关格式不正确。');
    if (typeof config.auto_harvest !== 'boolean') throw new Error('自动守护开关格式不正确。');
    if (typeof config.enabled !== 'boolean') throw new Error('总开关格式不正确。');
    if (typeof config.force_model !== 'string' || (config.force_model && !MODEL_PATTERN.test(config.force_model))) throw new Error('强制模型格式不正确。');
    if (typeof config.observe_exit_ip !== 'boolean') throw new Error('出口 IP 检测开关格式不正确。');
    if (typeof config.harvest_dial_proxy_url !== 'string') throw new Error('前置代理地址格式不正确。');
    if (/[{}]/.test(config.harvest_dial_proxy_url)) throw new Error('前置代理不能使用会话占位符。');
    for (const proxyValue of [config.dynamic_proxy_url, config.harvest_dial_proxy_url]) {
    if (typeof proxyValue !== 'string') throw new Error('动态代理地址格式不正确。');
    if (proxyValue) {
      try {
        if (proxyValue.length > 4096 || /[\r\n\t]/.test(proxyValue)) throw new Error();
        const expanded = proxyValue.replace(/\{(?:random|sid)\}/g, '123456');
        if (/[{}]/.test(expanded)) throw new Error();
        const url = new URL(expanded);
        if (!['http:', 'https:', 'socks5:', 'socks5h:'].includes(url.protocol) || !url.hostname || url.search || url.hash || (url.pathname && url.pathname !== '/')) throw new Error();
      } catch (_) { throw new Error('代理须为完整的 HTTP(S) 或 SOCKS5(H) 地址。'); }
    }
    }
    Object.keys(NUMBERS).forEach(function (key) {
      const bounds = NUMBERS[key];
      if (!Number.isInteger(config[key]) || config[key] < bounds[0] || config[key] > bounds[1]) {
        throw new Error(bounds[2] + '须为 ' + bounds[0] + '–' + bounds[1] + ' 之间的整数。');
      }
    });
    if (config.refresh_before_minutes >= config.ttl_minutes) throw new Error('提前续期必须小于票据有效期。');
    if (!Array.isArray(config.accounts) || config.accounts.length > 256) throw new Error('最多配置 256 个账号。');
    const ids = new Set();
    let totalModels = 0;
    config.accounts.forEach(function (account) {
      if (accountID(account.account_id) === null) throw new Error('账号 ID 须为正整数。');
      if (ids.has(account.account_id)) throw new Error('账号 ID ' + account.account_id + ' 重复。');
      ids.add(account.account_id);
      if (typeof account.enabled !== 'boolean') throw new Error('账号开关格式不正确。');
      if (!['pro', 'team'].includes(account.plan)) throw new Error('请选择 Pro 或 Team 套餐。');
      if (!Array.isArray(account.models) || !account.models.length || account.models.length > 16) throw new Error('每个账号须填写 1–16 个模型。');
      totalModels += account.models.length;
      const models = new Set();
      account.models.forEach(function (model) {
        if (typeof model !== 'string' || !MODEL_PATTERN.test(model)) throw new Error('模型须以 gpt- 开头，只能包含字母、数字、点、下划线和连字符，最长 99 个字符。');
        if (models.has(model)) throw new Error('同一账号的模型名称不能重复。');
        models.add(model);
      });
    });
    if (totalModels > 1024) throw new Error('最多配置 1024 个账号与模型组合。');
    if (config.enabled && config.accounts.some(function (account) { return account.enabled; }) && !config.dynamic_proxy_url) {
      throw new Error('启用账号前，请填写动态代理地址。');
    }
    return config;
  }
  function stateLabel(state) { return Object.prototype.hasOwnProperty.call(STATES, state) ? STATES[state] : ['未知状态', 'warning']; }
  function errorLabel(code) { return Object.prototype.hasOwnProperty.call(ERRORS, code) ? ERRORS[code] : '操作未完成，请检查账号与插件设置。'; }
  function redactError(value) {
    return String(value || '')
      .replace(/(?:https?|socks5h?):\/\/[^\s/]*@/gi, '[代理凭据已隐藏]@')
      .replace(/(?:x-codex-turn-state|authorization|access_token|refresh_token|api_key|password)\s*[:=]\s*[^\s,;]+/gi, '[敏感字段已隐藏]')
      .replace(/\beyJ[A-Za-z0-9_-]{15,}(?:\.[A-Za-z0-9_-]+){0,2}/g, '[票据已隐藏]')
      .slice(0, 400);
  }
  function parseStatus(result) {
    let status = result && result.status_json;
    if (typeof status === 'string') {
      try { status = JSON.parse(status); } catch (_) { throw new Error('宿主返回的状态格式不正确。'); }
    }
    if (!status || typeof status !== 'object' || Array.isArray(status)) status = {};
    return { host_ready: status.host_ready === true,
      resources_ready: status.resources_ready === true,
      actions_ready: status.actions_ready === true,
      manual_test: status.manual_test && typeof status.manual_test === 'object' ? status.manual_test : null,
      accounts: Array.isArray(status.accounts) ? status.accounts.filter(a => a && accountID(a.id) !== null).map(a => ({id: accountID(a.id), name: typeof a.name === 'string' ? a.name.slice(0, 256) : ''})) : [],
      proxies: Array.isArray(status.proxies) ? status.proxies.filter(p => p && accountID(p.id) !== null).map(p => ({id:accountID(p.id), name:typeof p.name === 'string' ? p.name.slice(0,256) : '', protocol:typeof p.protocol === 'string' ? p.protocol : '', host:typeof p.host === 'string' ? p.host : '', port:Number.isInteger(p.port) ? p.port : 0})) : [],
      account_ids: Array.isArray(status.account_ids) ? Array.from(new Set(status.account_ids.map(accountID).filter(function (id) { return id !== null; }))).sort(function (a, b) { return a - b; }) : [],
      tickets: Array.isArray(status.tickets) ? status.tickets.filter(function (ticket) { return ticket && accountID(ticket.account_id) !== null; }).slice(0, 4096) : [],
      events: Array.isArray(status.events) ? status.events.slice(-200) : [],
      message: redactError(MESSAGES[status.message] || status.message || result && result.message || '') };
  }
  function parseResources(result) {
    const source = result && result.resources && typeof result.resources === 'object' && !Array.isArray(result.resources) ? result.resources : {};
    const ids = function (values) {
      return Array.from(new Set((Array.isArray(values) ? values : []).map(accountID).filter(function (id) { return id !== null; })));
    };
    const accounts = Array.isArray(source.accounts) ? source.accounts.filter(function (account) {
      return account && accountID(account.id) !== null;
    }).map(function (account) {
      return { id: accountID(account.id), name: typeof account.name === 'string' ? account.name.slice(0, 256) : '', group_ids: ids(account.group_ids) };
    }) : [];
    const groups = Array.isArray(source.groups) ? source.groups.filter(function (group) {
      return group && accountID(group.id) !== null;
    }).map(function (group) {
      return { id: accountID(group.id), name: typeof group.name === 'string' ? group.name.slice(0, 256) : '' };
    }) : [];
    const proxies = Array.isArray(source.proxies) ? source.proxies.filter(function (proxy) {
      return proxy && accountID(proxy.id) !== null;
    }).map(function (proxy) {
      return { id: accountID(proxy.id), name: typeof proxy.name === 'string' ? proxy.name.slice(0, 256) : '',
        protocol: typeof proxy.protocol === 'string' ? proxy.protocol.slice(0, 32) : '',
        host: typeof proxy.host === 'string' ? proxy.host.slice(0, 256) : '',
        port: Number.isInteger(proxy.port) && proxy.port >= 0 && proxy.port <= 65535 ? proxy.port : 0 };
    }) : [];
    return { accounts: accounts, groups: groups, proxies: proxies };
  }
  function attemptRows(events) {
    const rows=new Map();
    events.forEach(entry=>{
      if(!entry||accountID(entry.account_id)===null)return;
      const route=entry.phase==='harvest'?'dynamic':entry.phase==='validate'||entry.phase==='restore'?'business':entry.phase==='connectivity'?entry.route:null;
      if(!['dynamic','business'].includes(route))return;
      const n=Number.isSafeInteger(entry.attempt)&&entry.attempt>0?entry.attempt:0;
      const key=[entry.round_id||'legacy',entry.account_id,entry.model,n,route].join('|');
      const previous=rows.get(key)||{};
      const merged=Object.assign({},previous,entry,{attempt:n,route,time:previous.time||entry.time});
      if(!entry.exit_ip){merged.exit_ip=previous.exit_ip||'';merged.country_code=previous.country_code||'';}
      rows.set(key,merged);
    });
    return Array.from(rows.values());
  }
  function countryLabel(code) {
    if(typeof code!=='string'||!/^[A-Z]{2}$/.test(code)||['XX','ZZ'].includes(code))return '国家未知';
    try {return new Intl.DisplayNames(['zh-CN'],{type:'region'}).of(code)+' ('+code+')';} catch(_){return code;}
  }
  function triggerLabel(trigger) {return {manual:'单账号手动',manual_all:'全部手动',automatic:'自动守护'}[trigger]||'';}
  function remainingText(seconds) {
    const value = Number(seconds);
    if (!Number.isFinite(value) || value <= 0) return '—';
    const minutes = Math.floor(value / 60);
    return minutes ? minutes + ' 分 ' + Math.floor(value % 60) + ' 秒' : Math.floor(value) + ' 秒';
  }

  function extractHTML(text) {
    const fenced = text.match(/```(?:html|svg)\s*([\s\S]*?)```/i);
    if (fenced) return fenced[1];
    const start = text.search(/<!doctype\s+html|<html[\s>]|<svg[\s>]/i);
    return start >= 0 ? text.slice(start) : '';
  }
  function previewDocument(html) {
    return '<!doctype html><meta http-equiv="Content-Security-Policy" content="default-src \'none\'; script-src \'unsafe-inline\'; style-src \'unsafe-inline\'; img-src data: blob:; font-src data:; connect-src \'none\'; form-action \'none\'; base-uri \'none\'"><meta name="referrer" content="no-referrer">' + html;
  }
  function start(global) {
    const document = global.document;
    const bridge = global.Sub2APIPluginBridge;
    const byID = function (id) { return document.getElementById(id); };
    let loaded = false;
    let busy = false;
    let dirty = false;
    let statusBusy = false;
    let closed = false;
    let pollTimer;
    let resizeObserver;
    let accounts = [];
    let accountNames = new Map();
    let resourceCatalog = null;
    let resourceBusy = false;
    let resourceError = false;
    let actionsReady = false;
    let actionBusy = false;
    let manualRunning = false;
    let manualOutput = '';
    let outputID = '';
    let accountCells = [];
    let accountStatusCells = [];
    let lastTickets = [];
    let proxyTest = null;
    let lastStatus = null;
    let manualPending = null;
    let manualLocalError = null;
    let manualSelectionInitialized = false;
    let lastManualKind = '';
    let manualRequestModel = '';
    let stopPending = false;
    const accountFeedback = new Map();
    function accountLabel(id) { return accountNames.has(id) ? accountNames.get(id) + ' · ID ' + id : 'ID ' + id; }
    function updateFrontMode() {
      byID('front-manual').hidden = byID('harvest-dial-proxy-mode').value !== 'manual';
      byID('front-managed').hidden = byID('harvest-dial-proxy-mode').value !== 'managed';
    }
    function renderPicker() {
      const group = byID('new-account-group'), choice = byID('new-account-account');
      const selectedGroup = group.value || 'all', selectedAccount = choice.value;
      group.replaceChildren(); choice.replaceChildren();
      const addOption = function (select, value, text) { const option = element('option', text); option.value = value; select.appendChild(option); };
      addOption(group, 'all', '全部分组');
      if (resourceCatalog) {
        addOption(group, 'ungrouped', '未分组');
        resourceCatalog.groups.forEach(g => addOption(group, String(g.id), g.name + ' · ID ' + g.id));
      }
      const validGroup = resourceCatalog && (selectedGroup === 'ungrouped' || resourceCatalog.groups.some(g => String(g.id) === selectedGroup));
      group.value = validGroup ? selectedGroup : 'all';
      group.disabled = !resourceCatalog;
      byID('new-account-id').hidden = !!resourceCatalog;
      byID('account-choice-field').hidden = !resourceCatalog;
      if (!resourceCatalog) {
        byID('account-discovery').textContent = resourceBusy ? '正在读取分组和账号…' : '此宿主暂未提供分组目录，可先输入账号 ID；更新宿主适配后支持下拉选择。';
        byID('add-account').disabled = false;
        return;
      }
      const inGroup = resourceCatalog.accounts.filter(a => group.value === 'all' || (group.value === 'ungrouped' ? !a.group_ids.length : a.group_ids.includes(Number(group.value))));
      const available = inGroup.filter(a => !accounts.some(saved => saved.account_id === a.id));
      addOption(choice, '', available.length ? '请选择账号' : (inGroup.length ? '本组账号已全部添加' : '本组没有可用账号'));
      available.forEach(a => addOption(choice, String(a.id), (a.name || '未命名账号') + ' · ID ' + a.id));
      choice.value = available.some(a => String(a.id) === selectedAccount) ? selectedAccount : '';
      choice.disabled = !available.length;
      byID('add-account').disabled = !choice.value || accounts.length >= 256;
      byID('account-discovery').textContent = (resourceError ? '目录刷新失败，暂显示上次列表。' : '') + '本组可添加 ' + available.length + ' 个账号。添加后默认关闭；分组仅用于筛选。';
    }
    async function refreshResources() {
      if (resourceBusy || closed) return;
      resourceBusy = true; renderPicker();
      try {
        if (!bridge.resources) throw Error('legacy host');
        const response = await bridge.resources();
        if (closed) return;
        if (!response.resources || !Array.isArray(response.resources.accounts) || !Array.isArray(response.resources.groups)) throw Error('invalid directory');
        resourceCatalog = parseResources(response); resourceError = false;
      } catch (_) { resourceError = true; }
      finally {
        resourceBusy = false;
        if (!closed) { renderStatus(lastStatus || parseStatus({})); renderPicker(); }
      }
    }
    function renderResources(status) {
      accountNames = new Map(status.accounts.map(a => [a.id, a.name || '未命名账号']));
      accountCells.forEach(cell => { cell.node.textContent = accountLabel(cell.id); });
      const select = byID('harvest-dial-proxy-id');
      const selected = select.value;
      select.replaceChildren();
      const empty = element('option', '请选择代理'); empty.value = ''; select.appendChild(empty);
      status.proxies.forEach(p => {
        const option = element('option', (p.name || '未命名代理') + ' · ID ' + p.id + ' · ' + p.protocol + '://' + p.host + ':' + p.port);
        option.value = p.id; select.appendChild(option);
      });
      if (selected && !status.proxies.some(p => String(p.id) === selected)) {
        const unavailable = element('option', 'ID ' + selected + '（暂不可用，请检查 IP 管理）'); unavailable.value = selected; select.appendChild(unavailable);
      }
      select.value = selected;
      byID('managed-proxy-option').disabled = !status.resources_ready;
      byID('proxy-discovery').textContent = status.resources_ready ? '可选 ' + status.proxies.length + ' 个代理，采集时读取最新地址。' : '当前宿主未提供代理列表，可手动填写。';
    }
    const numberIDs = { ttl_minutes: 'ttl-minutes', refresh_before_minutes: 'refresh-before-minutes',
      max_attempts: 'max-attempts', attempt_interval_seconds: 'attempt-interval-seconds', cooldown_seconds: 'cooldown-seconds' };
    function element(tag, text, className) {
      const node = document.createElement(tag);
      if (text !== undefined) node.textContent = String(text);
      if (className) node.className = className;
      return node;
    }
    function notice(message, kind) {
      const node = byID('notice');
      node.textContent = redactError(message);
      node.className = 'notice' + (kind ? ' ' + kind : '');
      node.hidden = !message;
    }
    function updateSaveState(text) {
      byID('save-state').textContent = text || (dirty ? '有未保存修改' : '配置已加载');
      byID('save-state').className = dirty ? 'dirty' : 'muted';
    }
    function markDirty() { if (loaded) { dirty = true; updateSaveState(); } }
    function setBusy(value) {
      busy = value;
      byID('config-fields').disabled = !loaded || busy;
      byID('save-config').disabled = !loaded || busy;
      byID('test-config').disabled = !loaded || busy;
      manualButtons();
    }
    function renderAccounts() {
      const body=byID('accounts-body');body.replaceChildren();accountCells=[];accountStatusCells=[];
      accounts.forEach(account=>{
        const row=element('tr');row.setAttribute('data-account-id',account.account_id);
        const nameCell=element('td');const name=element('div',accountLabel(account.account_id),'account-label');
        accountCells.push({id:account.account_id,node:name});nameCell.appendChild(name);
        const settings=element('details',undefined,'account-settings');const summary=element('summary',account.plan==='team'?'Team · 332 / 设置':'Pro · 292 / 设置');settings.appendChild(summary);
        const plan=element('select');plan.setAttribute('aria-label','账号 '+account.account_id+' 的套餐');
        [['pro','Pro · 292'],['team','Team · 332']].forEach(([value,label])=>{const o=element('option',label);o.value=value;plan.appendChild(o);});plan.value=account.plan;
        plan.addEventListener('change',()=>{account.plan=plan.value;summary.textContent=(plan.value==='team'?'Team · 332':'Pro · 292')+' / 待保存';markDirty();});settings.appendChild(plan);
        const models=element('input');models.type='text';models.value=account.models.join(', ');models.setAttribute('aria-label','账号 '+account.account_id+' 的模型');models.setAttribute('data-role','models');
        models.addEventListener('input',()=>{account.models=models.value.split(',').map(v=>v.trim()).filter(Boolean);markDirty();});settings.appendChild(models);
        const remove=element('button','移出插件列表','delete-button');remove.type='button';remove.addEventListener('click',()=>{accounts=accounts.filter(a=>a.account_id!==account.account_id);renderAccounts();markDirty();notice('已从草稿移出此账号，保存后生效；宿主账号不会删除。');});settings.appendChild(remove);nameCell.appendChild(settings);row.appendChild(nameCell);
        const enabledCell=element('td');const enabled=element('input');enabled.type='checkbox';enabled.checked=account.enabled===true;enabled.setAttribute('aria-label','启用账号 '+account.account_id);enabled.setAttribute('role','switch');
        const toggleFeedback=element('div','点击即保存','account-toggle-feedback');
        enabled.addEventListener('change',async()=>{
          if(busy||!loaded){enabled.checked=account.enabled;return;}
          const previous=account.enabled;const desired=enabled.checked;account.enabled=desired;setBusy(true);toggleFeedback.textContent='保存中…';
          try {
            const saved=normalizeConfig((await bridge.load()).config);const target=saved.accounts.find(a=>a.account_id===account.account_id);
            if(!target)throw Error('新账号请先点击保存设置。');target.enabled=desired;
            const response=await bridge.save(saved);const confirmed=normalizeConfig(response.config);
            if(confirmed.accounts.find(a=>a.account_id===account.account_id)?.enabled!==desired)throw Error('开关保存未确认，请重试。');
            accountFeedback.delete(account.account_id);toggleFeedback.textContent=desired?'已开启':'已关闭';
            try{dirty=JSON.stringify(normalizeConfig(formConfig()))!==JSON.stringify(confirmed);}catch(_){dirty=true;}
            updateSaveState(dirty?'其他修改待保存':'账号开关已保存');await refreshStatus();
          }catch(error){account.enabled=previous;enabled.checked=previous;toggleFeedback.textContent='未保存';accountFeedback.set(account.account_id,{text:redactError(error.message),kind:'error'});renderAccountStates(lastTickets);}
          finally{setBusy(false);}
        });enabledCell.appendChild(enabled);enabledCell.appendChild(toggleFeedback);row.appendChild(enabledCell);
        const statusCell=element('td');statusCell.setAttribute('aria-live','polite');row.appendChild(statusCell);
        const actions=element('td',undefined,'account-actions');const refresh=element('button','查找票据');refresh.type='button';
        refresh.addEventListener('click',async()=>{
          if(busy||actionBusy)return;
          accountFeedback.set(account.account_id,{text:'正在提交查找请求…',kind:'pending'});refresh.disabled=true;renderAccountStates(lastTickets);
          const response=await runAction({kind:'refresh',account_id:account.account_id});
          accountFeedback.set(account.account_id,{text:response?'已仅启动此账号，请查看上方进度。':lastActionError||'未能启动，请重试。',kind:response?'success':'error'});
          byID('activity-account').value=String(account.account_id);renderAccountStates(lastTickets);if(lastStatus)renderActivity(lastStatus);
        });
        const test=element('button','测试此账号','secondary');test.type='button';test.addEventListener('click',()=>{if(manualRunning||manualPending)return;byID('manual-account').value=String(account.account_id);byID('manual-model').value=account.models[0]||'gpt-6-astra';clearManualResult();byID('manual-panel').scrollIntoView?.({behavior:'smooth',block:'start'});});
        actions.appendChild(refresh);actions.appendChild(test);row.appendChild(actions);body.appendChild(row);
        accountStatusCells.push({id:account.account_id,node:statusCell,button:refresh,testButton:test});
      });renderAccountStates(lastTickets);byID('accounts-empty').hidden=accounts.length!==0;byID('account-count').textContent=accounts.length+' 个账号';renderPicker();
    }
    function applyConfig(input) {
      const config = normalizeConfig(input);
      byID('force-astra').checked = config.force_model === 'gpt-6-astra';
      byID('enabled').checked = config.enabled === true;
      byID('auto-harvest').checked=config.auto_harvest;
      byID('business-use-front').checked=config.business_use_front;
      byID('dynamic-proxy-url').value = config.dynamic_proxy_url;
      byID('harvest-dial-proxy-url').value = config.harvest_dial_proxy_url;
      byID('harvest-dial-proxy-mode').value = config.harvest_dial_proxy_mode;
      const selectedProxy = byID('harvest-dial-proxy-id');
      selectedProxy.replaceChildren();
      const selectedOption = element('option', config.harvest_dial_proxy_id ? 'ID ' + config.harvest_dial_proxy_id : '请选择代理');
      selectedOption.value = config.harvest_dial_proxy_id || ''; selectedProxy.appendChild(selectedOption); selectedProxy.value = selectedOption.value;
      updateFrontMode();
      byID('observe-exit-ip').checked = config.observe_exit_ip;
      byID('allow-without-ticket').checked = config.allow_without_ticket;
      Object.keys(numberIDs).forEach(function (key) { byID(numberIDs[key]).value = config[key]; });
      accounts = config.accounts;
      renderAccounts();
      dirty = false;
      updateSaveState();
    }
    function formConfig() {
      const config = { force_model: byID('force-astra').checked ? 'gpt-6-astra' : '', enabled: byID('enabled').checked, auto_harvest:byID('auto-harvest').checked, business_use_front:byID('business-use-front').checked, allow_without_ticket: byID('allow-without-ticket').checked, dynamic_proxy_url: byID('dynamic-proxy-url').value.trim(), harvest_dial_proxy_mode: byID('harvest-dial-proxy-mode').value, harvest_dial_proxy_id: byID('harvest-dial-proxy-mode').value === 'managed' ? Number(byID('harvest-dial-proxy-id').value) : 0, harvest_dial_proxy_url: byID('harvest-dial-proxy-mode').value === 'manual' ? byID('harvest-dial-proxy-url').value.trim() : '', observe_exit_ip: byID('observe-exit-ip').checked };
      Object.keys(numberIDs).forEach(function (key) {
        const raw = byID(numberIDs[key]).value.trim();
        config[key] = raw === '' ? NaN : Number(raw);
      });
      config.accounts = accounts.map(function (account) { return {
        account_id: account.account_id, enabled: account.enabled, plan: account.plan, models: account.models.slice()
      }; });
      return validateConfig(config);
    }
    function renderAccountStates(tickets) {
      accountStatusCells.forEach(cell => {
        const rows = tickets.filter(t => Number(t.account_id) === cell.id);
        cell.node.replaceChildren();
        rows.forEach(ticket => {
          const state = stateLabel(ticket.state), line = element('div',undefined,'account-state');
          line.appendChild(element('span',state[0],'badge '+state[1]));
          if(triggerLabel(ticket.trigger))line.appendChild(element('div',triggerLabel(ticket.trigger),'muted'));
          if(typeof ticket.exit_ip==='string'&&/^[0-9a-fA-F:.]{2,45}$/.test(ticket.exit_ip))line.appendChild(element('div','动态出口：'+ticket.exit_ip+' · '+countryLabel(ticket.country_code),'muted'));
          if(rows.length>1 && MODEL_PATTERN.test(ticket.model)) line.appendChild(element('div',ticket.model,'muted'));
          if(ticket.remaining_seconds>0)line.appendChild(element('div','剩余 '+remainingText(ticket.remaining_seconds),'muted'));
          if(Number.isSafeInteger(ticket.attempts)&&ticket.attempts>0)line.appendChild(element('div','本轮第 '+ticket.attempts+' 次尝试','muted'));
          if(ticket.last_error){
            let reason=errorLabel(ticket.last_error);
            if(ticket.last_error==='model_mismatch'&&['cooldown','error'].includes(ticket.state))reason='本轮已结束：上游请求已返回，但模型仍被路由，未取得有效票据。';
            if(ticket.last_error==='dynamic_connectivity_failed'&&['cooldown','error'].includes(ticket.state))reason='本轮已结束：最后一次动态出口检测未通过。可稍后重新查找。';
            line.appendChild(element('div',reason,String(ticket.last_error).startsWith('checking_')?'muted':'account-alert'));
          }
          cell.node.appendChild(line);
        });
        if(!rows.length)cell.node.textContent='保存后显示状态';
        const feedback=accountFeedback.get(cell.id);if(feedback)cell.node.appendChild(element('div',feedback.text,'account-feedback '+feedback.kind));
        const running=rows.some(t=>['harvesting','renewing','checking_proxy'].includes(t.state));
        cell.button.disabled=!actionsReady || busy || actionBusy || running;
        cell.testButton.disabled=busy||actionBusy||manualRunning||!!manualPending;
        cell.button.textContent=running?'正在查找…':'查找票据';
      });
    }
    function renderStatus(status) {
      if (resourceCatalog) status = Object.assign({}, status, { resources_ready: true,
        accounts: resourceCatalog.accounts, proxies: resourceCatalog.proxies,
        account_ids: resourceCatalog.accounts.map(a => a.id) });
      lastStatus=status;
      renderResources(status);
      renderManual(status);
      const connection = byID('connection-status');
      connection.textContent = status.host_ready ? '宿主已连接' : '等待宿主初始化';
      connection.className = 'badge ' + (status.host_ready ? 'success' : 'warning');
      byID('status-summary').textContent = status.message || (status.host_ready ? '状态已更新' : '等待宿主提供账号信息；可先保存配置。');
      const options = byID('detected-accounts'); options.replaceChildren();
      status.account_ids.forEach(function (id) { const option = element('option', accountLabel(id)); option.setAttribute('label', accountLabel(id)); option.value = id; options.appendChild(option); });
      byID('account-discovery').textContent = status.account_ids.length ? '发现 ' + status.account_ids.length + ' 个账号。' + (status.resources_ready ? '输入 ID 或按名称选择。' : '宿主未提供名称，请在账号页核对。') : '暂未发现账号 ID，也可以手动填写。宿主不会向此页面提供账号 Token。';
      renderPicker();
      lastTickets = status.tickets; renderAccountStates(lastTickets);manualButtons();
      renderActivity(status);
    }
    function renderActivity(status) {
      const filter=byID('activity-account'),selected=filter.value;filter.replaceChildren();
      const all=element('option','全部账号');all.value='';filter.appendChild(all);
      status.account_ids.forEach(id=>{const o=element('option',accountLabel(id));o.value=id;filter.appendChild(o);});filter.value=selected;
      const logs = byID('activity-body'); logs.replaceChildren();
      attemptRows(status.events).reverse().forEach(function (entry) {
        if (!entry || accountID(entry.account_id) === null || (filter.value && String(entry.account_id)!==filter.value)) return;
        const row = element('tr');
        const time = Number.isFinite(Date.parse(entry.time)) ? new Date(entry.time).toLocaleTimeString('zh-CN') : '—';
        const accountCell=element('td',time+' / '+accountLabel(accountID(entry.account_id)));if(triggerLabel(entry.trigger))accountCell.appendChild(element('div',triggerLabel(entry.trigger),'muted'));row.appendChild(accountCell);
        const phase = { connectivity:entry.route==='business'?'业务出口检查':entry.route==='dynamic'?'动态出口检查':'代理连通性检查', harvest: '动态采集', validate: '业务出口复验', restore: '恢复复验', collection: '采集轮次', watchdog: '异常守护' }[entry.phase] || '—';
        row.appendChild(element('td', phase + (entry.chained ? ' · 前置代理' : '') + (Number.isSafeInteger(entry.attempt) && entry.attempt > 0 ? ' · 第 ' + entry.attempt + ' 次' : '')));
        const ip = typeof entry.exit_ip === 'string' && /^[0-9a-fA-F:.]{2,45}$/.test(entry.exit_ip) ? entry.exit_ip : '—';
        const exitCell=element('td',ip==='—'?'未取得 IP':ip);exitCell.appendChild(element('div',countryLabel(entry.country_code),'muted'));row.appendChild(exitCell);
        const resultCode=entry.result==='upstream_rejected'&&entry.http_status===401?'upstream_unauthorized':entry.result;
        const result = { dynamic_connectivity_failed:'本次动态出口检测未通过', connectivity_ok:'IP 检测站连通（尚未验证上游授权）', started: '开始', ip_observed: '已检测出口', ip_check_failed: '出口检测失败，继续模型探测', model_matched: '成功 · 返回模型匹配', model_mismatch: '返回模型不匹配', incomplete_response: '响应未完整结束', transport_failed: '代理连接或传输失败', transport_unavailable: '代理配置不可用', ready: '票据可用', cancelled: '已取消' }[resultCode] || errorLabel(resultCode);
        const cell = element('td', result);
        const details = [];
        if(entry.reused_proxy_session===true)details.push('沿用上方测通的会话');
        if (Number.isSafeInteger(entry.http_status) && entry.http_status > 0) details.push('HTTP ' + entry.http_status);
        if (typeof entry.actual_model === 'string' && MODEL_PATTERN.test(entry.actual_model)) details.push(entry.actual_model);
        if (Number.isSafeInteger(entry.state_bytes) && entry.state_bytes >= 0) details.push('STATE ' + entry.state_bytes + ' 字节');
        if (Number.isSafeInteger(entry.duration_ms) && entry.duration_ms >= 0) details.push((entry.duration_ms / 1000).toFixed(1) + ' 秒');
        cell.appendChild(element('div', details.join(' · '), 'muted')); row.appendChild(cell); logs.appendChild(row);
      });
      byID('activity-empty').hidden = logs.children.length !== 0;
    }
    function safeModelName(value) {
      const model = typeof value === 'string' ? value.trim() : '';
      return MODEL_PATTERN.test(model) ? model : '';
    }
    function hideModelComparison() {
      const panel = byID('manual-model-comparison');
      panel.hidden = true;
      panel.className = 'model-comparison';
      manualRequestModel = '';
      byID('manual-request-model').textContent = '未提供';
      byID('manual-actual-model').textContent = '等待返回';
      byID('manual-model-match').textContent = '';
      byID('manual-model-match').className = 'model-match';
    }
    function renderModelComparison(result) {
      const panel = byID('manual-model-comparison');
      if (!result || result.kind !== 'test') {
        hideModelComparison();
        return;
      }
      const reportedRequestModel = safeModelName(result.model);
      if (reportedRequestModel) manualRequestModel = reportedRequestModel;
      const requestModel = reportedRequestModel || safeModelName(manualRequestModel);
      const actualModel = safeModelName(result.actual_model);
      const requestLabel = requestModel || '未提供';
      const actualLabel = actualModel || (result.running ? '等待返回' : result.error ? '未返回' : '未提供');
      byID('manual-request-model').textContent = requestLabel;
      byID('manual-actual-model').textContent = actualLabel;
      byID('manual-model-note').textContent = '模型名称一致仅用于核对路由，不代表能力评分。';
      panel.hidden = false;
      let status = '等待实际返回模型…';
      let statusClass = 'pending';
      if (result.error) {
        status = '未完成，无法比较模型名称';
        statusClass = 'error';
      } else if (result.running || !result.complete) {
        status = actualModel ? '已收到模型名，等待测试完成…' : '等待实际返回模型…';
        statusClass = 'pending';
      } else if (!actualModel) {
        status = '未提供实际返回模型，无法确认是否匹配';
        statusClass = 'warning';
      } else if (!requestModel) {
        status = '已返回模型，但未提供请求模型，无法比较';
        statusClass = 'warning';
      } else {
        const matches = typeof result.matches === 'boolean' ? result.matches : requestModel === actualModel;
        status = matches ? '✓ 模型名称一致 · 仅路由指标' : '✕ 模型名称不一致';
        statusClass = matches ? 'match' : 'mismatch';
      }
      const statusNode = byID('manual-model-match');
      statusNode.textContent = status;
      statusNode.className = 'model-match ' + statusClass;
    }
    function manualButtons() {
      const pending=!!manualPending;const locked=manualRunning||pending;const unavailable=!actionsReady||busy||actionBusy;
      accountStatusCells.forEach(cell=>{cell.button.disabled=unavailable||lastTickets.some(t=>Number(t.account_id)===cell.id&&['harvesting','renewing','checking_proxy'].includes(t.state));cell.testButton.disabled=unavailable||locked;});
      byID('refresh-all').disabled=unavailable||!lastTickets.some(t=>t.state!=='disabled'&&t.state!=='waiting_account');
      byID('proxy-test').disabled=unavailable||locked;
      byID('manual-test').disabled=unavailable||locked;
      byID('manual-ip').disabled=unavailable||locked;
      byID('manual-test').textContent=locked&&lastManualKind==='test'?'测试中…':'测试模型';
      const route=byID('manual-route').value;
      byID('manual-ip').textContent=locked&&lastManualKind==='ip'?'检测中…':route==='dynamic'?'检测动态出口':route==='direct'?'检测直连出口':'检测业务出口';
      byID('manual-cancel').hidden=!locked||lastManualKind==='proxy_test';
      byID('manual-cancel').disabled=!actionsReady||!manualRunning||actionBusy||stopPending;
      byID('manual-cancel').textContent=stopPending?'正在停止…':'停止测试';
      ['manual-account','manual-model','manual-prompt','manual-use-state','manual-route','manual-expected-ip','quick-prompt','pelican-prompt','candy-prompt'].forEach(id=>{byID(id).disabled=locked;});
    }
    function manualFeedback(title,text,kind) {
      byID('manual-result-title').textContent=title;
      byID('manual-status').textContent=text;
      byID('manual-result').className='result-panel'+(kind?' '+kind:'');
    }
    function clearManualResult() {
      manualLocalError=null;
      manualRequestModel='';
      manualOutput='';outputID='';byID('manual-output').textContent='';byID('manual-response').hidden=true;
      byID('preview-html').hidden=true;byID('preview-html').textContent='查看 HTML';byID('manual-output').hidden=false;byID('html-preview').hidden=true;byID('html-preview').srcdoc='';
      hideModelComparison();
      manualFeedback('准备测试',accountLabel(accountID(byID('manual-account').value))+' · 点击测试，不会自动发送请求。','');
    }
    function proxyForm() {
      const mode=byID('harvest-dial-proxy-mode').value;
      return {dynamic_proxy_url:byID('dynamic-proxy-url').value.trim(),harvest_dial_proxy_mode:mode,
        harvest_dial_proxy_url:mode==='manual'?byID('harvest-dial-proxy-url').value.trim():'',
        harvest_dial_proxy_id:mode==='managed'?Number(byID('harvest-dial-proxy-id').value):0};
    }
    function showProxyResult(result) {
      const node=byID('proxy-test-status');
      const owned=proxyTest && proxyTest.id===result.id;
      const sessionUntil=Date.parse(result.proxy_session_expires_at||'');
      const sessionInfo=Number.isFinite(sessionUntil)&&sessionUntil>Date.now()?'首次手动查找优先沿用本次会话（短时保留）':Number.isFinite(sessionUntil)?'测通会话已过期，下次会换新出口':'仅本次出口检测通过';
      const scope='；账号授权和上游可用性需另行验证';
      const details='出口 IP：'+result.exit_ip+' · '+countryLabel(result.country_code)+' · '+(result.duration_ms/1000).toFixed(1)+' 秒';
      if(proxyTest && proxyTest.error) {node.textContent=proxyTest.error;node.className='proxy-result error';return;}
      if(result.running) {node.textContent='◌ 正在测试代理连通性，通过后自动保存并生效…';node.className='proxy-result pending';return;}
      if(!result.complete) {node.textContent='✕ 代理测试失败，保留原配置 · '+redactError(result.error||'未取得出口 IP');node.className='proxy-result error';return;}
      if(owned && proxyTest.phase==='saved') {
        if(JSON.stringify(proxyForm())!==JSON.stringify(proxyTest.proxy)) {node.textContent='代理填写内容已更改，请重新测试使新配置生效。';node.className='proxy-result pending';return;}
        node.textContent='✓ 代理已自动保存并生效 · '+sessionInfo+scope+' · '+details;node.className='proxy-result success';return;
      }
      if(owned) {node.textContent='◌ 代理连通，正在保存并生效… · '+details;node.className='proxy-result pending';return;}
      node.textContent='✓ 上次代理连通性结果 · '+sessionInfo+scope+' · '+details;node.className='proxy-result success';
    }
    async function applyTestedProxy(result) {
      const pending=proxyTest;
      if(!pending || pending.phase!=='testing' || !pending.id) return;
      if(!result || result.id!==pending.id || result.running) {
        if(Date.now()>pending.deadline) {pending.phase='error';pending.error='未能确认本次测试结果，未自动保存，请重试。';setBusy(false);showProxyResult({id:pending.id});}
        return;
      }
      if(!result.complete) {pending.phase='failed';setBusy(false);return;}
      pending.phase='saving';showProxyResult(result);
      try {
        if(JSON.stringify(proxyForm())!==JSON.stringify(pending.proxy))throw new Error('代理填写内容已改变，未自动保存；请重新测试。');
        // Reload persisted config and change only the four proxy fields. Other
        // unsaved form fields and account switches never enter this save.
        const saved=normalizeConfig((await bridge.load()).config);
        Object.assign(saved,pending.proxy);
        const response=await bridge.save(saved);
        const confirmed=normalizeConfig(response.config);
        if(Object.keys(pending.proxy).some(key=>confirmed[key]!==pending.proxy[key]))throw new Error('保存结果与本次测试配置不一致，请重新打开页面核对。');
        pending.phase='saved';
        try {dirty=JSON.stringify(normalizeConfig(formConfig()))!==JSON.stringify(confirmed);} catch(_){dirty=true;}
        updateSaveState(dirty?'代理已自动生效；其他修改尚未保存':'代理已自动保存并生效');
        notice('代理测试通过，已自动保存这组代理配置。可以直接点击账号查找票据。','success');
      } catch(error) {
        pending.phase='error';pending.error='代理连通，但未能确认保存成功：'+redactError(error.message)+' 请重试或重新打开配置页核对。';
      } finally {if(!closed){setBusy(false);showProxyResult(result);}}
    }
    function renderManual(status) {
      actionsReady=status.actions_ready;
      const result=status.manual_test;const select=byID('manual-account');const selected=select.value;select.replaceChildren();
      status.account_ids.forEach(id=>{const o=element('option',accountLabel(id));o.value=id;select.appendChild(o);});
      select.value=selected&&status.account_ids.includes(Number(selected))?selected:(status.account_ids.length?String(status.account_ids[0]):'');
      if(!manualSelectionInitialized&&status.account_ids.length){if(result&&result.kind!=='proxy_test'&&status.account_ids.includes(Number(result.account_id)))select.value=String(result.account_id);manualSelectionInitialized=true;}
      manualRunning=!!(result&&result.running);if(!manualPending||manualPending.id===result?.id)lastManualKind=result?.kind||lastManualKind;
      byID('manual-capability').textContent=actionsReady?'测试走账号固定业务出口；未开启账号或没有可用票据时，不会注入 STATE。':'请先启用插件，再使用测试功能。';
      if(manualPending){
        if(manualPending.id&&result?.id===manualPending.id){manualRequestModel=manualPending.model||manualRequestModel;manualPending=null;}
        else {manualButtons();return;}
      }
      manualButtons();
      if(!result){clearManualResult();return;}
      if(result.kind==='proxy_test'){hideModelComparison();showProxyResult(result);return;}
      if(manualLocalError){hideModelComparison();manualFeedback(manualLocalError.title,manualLocalError.text,'error');return;}
      if(Number(result.account_id)!==Number(select.value)){clearManualResult();return;}
      renderModelComparison(result);
      if(outputID!==result.id){outputID=result.id;byID('html-preview').hidden=true;byID('html-preview').srcdoc='';byID('manual-output').hidden=false;byID('preview-html').textContent='查看 HTML';}
      const stopped=/已取消/.test(result.error||'');
      const title=result.running?(stopPending?'正在停止测试…':'正在'+(result.kind==='ip'?'检测出口…':'等待模型回答…')):result.error?(stopped?'测试已停止':'测试失败'):result.kind==='ip'?'出口连通':result.complete?(result.matches?'模型匹配':'模型不匹配'):'响应未完成';
      const kind=result.running?'pending':result.error?'error':result.kind==='ip'||result.matches?'success':'warning';
      const details=[accountLabel(accountID(result.account_id))];
      if(result.model)details.push('请求 '+result.model);
      if(result.actual_model)details.push('返回 '+result.actual_model);
      if(result.kind==='test')details.push(result.injected?'已注入 STATE':'未注入 STATE');
      if(result.exit_ip)details.push(result.exit_ip+' · '+countryLabel(result.country_code));
      if(result.http_status)details.push('HTTP '+result.http_status);
      if(result.duration_ms)details.push((result.duration_ms/1000).toFixed(1)+' 秒');
      if(typeof result.ip_matches==='boolean')details.push(result.ip_matches?'与预期 IP 一致':'与预期 IP 不同');
      if(result.error)details.push(redactError(result.error));
      manualFeedback(title,details.join(' · '),kind);
      manualOutput=typeof result.text==='string'?result.text.slice(0,262144):'';byID('manual-output').textContent=manualOutput;
      byID('manual-response').hidden=!manualOutput;byID('preview-html').hidden=!extractHTML(manualOutput);
      if(!result.running){stopPending=false;manualButtons();}
    }
    let lastActionError='';
    async function runAction(action,feedback) {
      if (actionBusy) return;
      lastActionError='';
      if (!actionsReady || !bridge.action) {notice('此操作需要新版宿主适配，并启用插件。','error');return;}
      actionBusy=true;manualButtons();
      try {const response=await bridge.action(action);if(response.result && response.result.accepted===false)throw new Error(response.result.message);notice(response.result.message,'success');if(feedback)feedback.textContent=response.result.message;await refreshStatus();return response;}
      catch(error){lastActionError=redactError(error.message);notice(error.message,'error');if(feedback){feedback.textContent=redactError(error.message);if(feedback===byID('proxy-test-status'))feedback.className='proxy-result error';}}finally{actionBusy=false;manualButtons();accountStatusCells.forEach(cell=>{cell.button.disabled=!actionsReady||busy||lastTickets.some(t=>Number(t.account_id)===cell.id&&['harvesting','renewing','checking_proxy'].includes(t.state));});}
    }
    async function testAction(kind) {
      if(busy||actionBusy||manualRunning||manualPending)return;
      const id=accountID(byID('manual-account').value),model=byID('manual-model').value.trim(),prompt=byID('manual-prompt').value.trim();
      if(!id){manualFeedback('请选择账号','先选择要测试的账号。','error');return;}
      if(kind==='test'&&(!MODEL_PATTERN.test(model)||!prompt)){manualFeedback('请补全测试内容','填写有效模型名称和提示词后再试。','error');return;}
      clearManualResult();manualRequestModel=kind==='test'?model:'';manualPending={id:null,account:id,model:kind==='test'?model:'',deadline:Date.now()+25000};lastManualKind=kind;
      manualFeedback(kind==='ip'?'正在检测出口…':'正在提交模型测试…',accountLabel(id)+' · 请稍候，可在开始后停止测试。','pending');manualButtons();
      const response=await runAction({kind,account_id:id,model,prompt,use_state:byID('manual-use-state').checked,route:byID('manual-route').value,expected_ip:byID('manual-expected-ip').value.trim()});
      if(!response){manualPending=null;manualLocalError={title:'未能开始测试',text:lastActionError||'请确认插件已启用后重试。'};manualFeedback(manualLocalError.title,manualLocalError.text,'error');manualButtons();return;}
      if(!response.request_id){manualPending=null;manualFeedback('无法确认测试进度','请刷新状态后再操作。','error');manualButtons();return;}
      manualPending.id=response.request_id;
      await refreshStatus();
    }
    byID('refresh-all').addEventListener('click',()=>{runAction({kind:'refresh_all'},byID('refresh-all-status'));});
    byID('proxy-test').addEventListener('click',async()=>{
      if(busy||!loaded||actionBusy||manualRunning)return;
      proxyTest={proxy:proxyForm(),id:null,phase:'starting',deadline:Date.now()+45000};
      const pending=proxyTest;setBusy(true);
      byID('proxy-test-status').textContent='◌ 正在测试，通过后自动保存代理配置…';byID('proxy-test-status').className='proxy-result pending';
      const response=await runAction({kind:'proxy_test',proxy:pending.proxy},byID('proxy-test-status'));
      if(closed)return;
      if(!response || !response.request_id) {
        pending.phase='error';pending.error='未能启动或关联本次代理测试，未自动保存，请重试。';setBusy(false);showProxyResult({id:null});return;
      }
      pending.id=response.request_id;pending.phase='testing';await refreshStatus();
    });
    byID('manual-test').addEventListener('click',()=>testAction('test'));
    byID('manual-ip').addEventListener('click',()=>testAction('ip'));
    byID('manual-cancel').addEventListener('click',async()=>{stopPending=true;manualButtons();manualFeedback('正在停止测试…','取消请求已提交，等待连接结束。','pending');const response=await runAction({kind:'cancel_test'});if(!response){stopPending=false;manualFeedback('未能停止测试',lastActionError||'请重试。','error');manualButtons();}});
    byID('quick-prompt').addEventListener('click',()=>{byID('manual-prompt').value='只回复 OK';});
    byID('manual-account').addEventListener('change',()=>{const a=accounts.find(a=>a.account_id===Number(byID('manual-account').value));if(a)byID('manual-model').value=a.models[0]||'gpt-6-astra';clearManualResult();});
    byID('manual-model').addEventListener('input',()=>{if(!manualRunning&&!manualPending)clearManualResult();});
    byID('manual-route').addEventListener('change',manualButtons);
    byID('activity-account').addEventListener('change',()=>{if(lastStatus)renderActivity(lastStatus);});
    byID('pelican-prompt').addEventListener('click',()=>{byID('manual-prompt').value='创建一个 HTML，内容是 SVG 绘制一个鹈鹕骑自行车的 2D 动画，不要使用任何技能。返回完整可运行的 HTML。';});
    byID('candy-prompt').addEventListener('click',()=>{byID('manual-prompt').value=`你正在参加一个可复核的逻辑推理测试。不使用任何外部工具。

黑色袋子里有三种口味的糖果：苹果味、桃子味、西瓜味；每种口味都有圆形和五角星形两种形状，形状可以靠手感辨别。糖果数量如下：

        苹果味  桃子味  西瓜味
圆形       5      5      5
五角星形   5      5      5

现在从袋中不放回地盲取糖果。要算“成功”，手中必须同时出现以下两种糖果中的至少一种组合：
1. 圆形苹果味 + 五角星形桃子味；
2. 圆形桃子味 + 五角星形苹果味。

问题：最少取出多少颗糖果，才能保证一定成功？请给出简短、可核验的最坏情况证明。

判定方法提示（不是答案）：请用“最大失败集合 + 1”的方法求最小保证数量。要分别检查同时避开两种成功组合的四种可能，不要只给出一个足够但不一定最小的分情况上界。

输出协议（必须严格遵守）：
- 只输出一个 JSON 对象；不要输出 Markdown、代码围栏、前后解释或其他文字。
- JSON 必须有两个字段：final_answer（整数）和 reason（字符串）。
- final_answer 只能填写你推理得到的最小数量；不要猜测或照抄任何预设答案。
- reason 用不超过两句话说明“为什么少一颗仍可能失败，以及为什么再多一颗就一定成功”。`;});
    byID('preview-html').addEventListener('click',()=>{
      const html=extractHTML(manualOutput);
      if(!html){notice('回答中尚未找到 HTML 或 SVG，可查看源码。','error');return;}
      const frame=byID('html-preview');const show=frame.hidden;frame.hidden=!show;byID('manual-output').hidden=show;byID('preview-html').textContent=show?'查看源码':'查看 HTML';if(show){frame.srcdoc=previewDocument(html);frame.scrollIntoView?.({behavior:'smooth',block:'nearest'});}
    });
    function checkProgressTimeouts() {
      if(proxyTest&&proxyTest.phase==='testing'&&Date.now()>proxyTest.deadline){proxyTest.phase='error';proxyTest.error='未能确认代理测试结果，未自动保存。请刷新后重试。';setBusy(false);showProxyResult({id:proxyTest.id});}
      if(manualPending&&Date.now()>manualPending.deadline){manualPending=null;manualFeedback('暂时无法读取测试进度','不代表测试已停止。请刷新状态，或重新打开此页面。','error');manualButtons();}
    }
    async function refreshStatus() {
      if (closed || statusBusy || !bridge) return;
      statusBusy = true; byID('refresh-status').disabled = true;
      try {
        const response = await bridge.status();
        if (!closed) {const status=parseStatus(response.result);renderStatus(status);await applyTestedProxy(status.manual_test);}
      } catch (error) {
        if (!closed) {
          byID('connection-status').textContent = '状态暂不可用';
          byID('connection-status').className = 'badge warning';
          byID('status-summary').textContent = redactError(error.message);
        }
      } finally { statusBusy = false;checkProgressTimeouts(); if (!closed) byID('refresh-status').disabled = false; }
    }
    byID('harvest-dial-proxy-mode').addEventListener('change', updateFrontMode);
    ['dynamic-proxy-url','harvest-dial-proxy-mode','harvest-dial-proxy-id','harvest-dial-proxy-url'].forEach(id=>{byID(id).addEventListener('input',()=>{if(proxyTest&&proxyTest.phase==='saved'&&JSON.stringify(proxyForm())!==JSON.stringify(proxyTest.proxy)){byID('proxy-test-status').textContent='代理填写内容已更改，请重新测试使新配置生效。';byID('proxy-test-status').className='proxy-result pending';}});});
    function configEdited(event) { if (['new-account-group','new-account-account','new-account-id'].includes(event.target?.id)) return; markDirty(); }
    byID('config-form').addEventListener('input', configEdited);
    byID('config-form').addEventListener('change', configEdited);
    byID('new-account-group').addEventListener('change', () => { byID('new-account-account').value = ''; renderPicker(); });
    byID('new-account-account').addEventListener('change', renderPicker);
    async function saveConfig(event) {
      event.preventDefault(); if (busy || !loaded) return;
      let config;
      try { config = formConfig(); } catch (error) { notice(error.message, 'error'); return; }
      setBusy(true); updateSaveState('正在保存…');
      try {
        const response = await bridge.save(config);
        if (closed) return;
        applyConfig(response.config); updateSaveState('已保存');
        notice(config.enabled ? '设置已保存。仅开启的账号会参与票据获取与注入。' : '设置已保存。STATE Kit 已关闭，正常请求继续转发。', 'success');
        await refreshStatus();
      } catch (error) { if (!closed) { notice(error.message, 'error'); updateSaveState('保存未确认；重新打开配置页可核对宿主结果。'); } }
      finally { if (!closed) setBusy(false); }
    }
    // The host iframe does not grant allow-forms: save via Bridge on an explicit
    // button click instead of relying on sandbox-blocked native form submission.
    byID('save-config').addEventListener('click', saveConfig);
    byID('config-form').addEventListener('submit', saveConfig);
    byID('add-account').addEventListener('click', function () {
      const id = accountID(byID(resourceCatalog ? 'new-account-account' : 'new-account-id').value);
      if (id === null) { notice(resourceCatalog ? '请先选择账号。' : '请输入有效的正整数账号 ID。', 'error'); return; }
      if (resourceCatalog && !resourceCatalog.accounts.some(a => a.id === id)) { notice('账号已不在目录中，请刷新后重试。', 'error'); return; }
      if (accounts.some(function (account) { return account.account_id === id; })) { notice('此账号已在列表中。', 'error'); return; }
      if (accounts.length >= 256) { notice('最多配置 256 个账号。', 'error'); return; }
      accounts.push({ account_id: id, enabled: false, plan: 'pro', models: ['gpt-6-astra'] });
      renderAccounts(); markDirty(); byID('new-account-id').value = ''; notice('已添加 ' + accountLabel(id) + '，默认关闭。确认套餐并保存设置后，可手动开启。');
    });
    byID('new-account-id').addEventListener('keydown', function (event) {
      if (event.key === 'Enter') { event.preventDefault(); byID('add-account').click(); }
    });
    byID('toggle-proxy').addEventListener('click', function () {
      const input = byID('dynamic-proxy-url'); const reveal = input.type === 'password';
      input.type = reveal ? 'text' : 'password'; byID('toggle-proxy').textContent = reveal ? '隐藏' : '显示';
      byID('toggle-proxy').setAttribute('aria-pressed', String(reveal));
    });
    byID('toggle-front-proxy').addEventListener('click', function () {
      const input = byID('harvest-dial-proxy-url'); const reveal = input.type === 'password';
      input.type = reveal ? 'text' : 'password'; byID('toggle-front-proxy').textContent = reveal ? '隐藏' : '显示';
      byID('toggle-front-proxy').setAttribute('aria-pressed', String(reveal));
    });
    byID('test-config').addEventListener('click', async function () {
      if (busy || !loaded) return;
      setBusy(true); notice('正在检查已保存配置；未保存修改不参与检查。');
      try {
        const response = await bridge.test();
        if (!closed) notice((response.result && response.result.message || '已保存配置检查完成。') + (dirty ? ' 当前表单还有未保存修改。' : ''), 'success');
      } catch (error) { if (!closed) notice(error.message, 'error'); }
      finally { if (!closed) setBusy(false); }
    });
    byID('refresh-status').addEventListener('click', async () => { await Promise.all([refreshResources(), refreshStatus()]); });
    function resize() { try { bridge.resize(document.documentElement.scrollHeight); } catch (_) { /* Context may already be closed. */ } }
    function stop() {
      if (closed) return;
      closed = true; global.clearInterval(pollTimer);
      if (resizeObserver) resizeObserver.disconnect();
      if (bridge) bridge.dispose();
      global.removeEventListener('pagehide', stop);
    }
    global.addEventListener('pagehide', stop);
    (async function () {
      try {
        if (!bridge) throw new Error('配置桥接未加载，请重新打开插件配置页。');
        bridge.ready();
        const response = await bridge.load();
        if (closed) return;
        applyConfig(response.config); loaded = true; setBusy(false); resize();
        if (global.ResizeObserver) { resizeObserver = new global.ResizeObserver(resize); resizeObserver.observe(document.body); }
        await Promise.all([refreshResources(), refreshStatus()]);
        if (!closed) pollTimer = global.setInterval(function () { if (document.visibilityState !== 'hidden') refreshStatus(); }, 5000);
      } catch (error) { if (!closed) { notice(error.message, 'error'); updateSaveState('配置未加载'); byID('connection-status').textContent = '连接失败'; } }
    })();
    return { stop: stop, refreshStatus: refreshStatus };
  }
  return { attemptRows:attemptRows, countryLabel:countryLabel, extractHTML:extractHTML, previewDocument:previewDocument, DEFAULT_CONFIG: DEFAULT_CONFIG, normalizeConfig: normalizeConfig, validateConfig: validateConfig,
    accountID: accountID, parseResources: parseResources, parseStatus: parseStatus, stateLabel: stateLabel, errorLabel: errorLabel, redactError: redactError, remainingText: remainingText, start: start };
});
