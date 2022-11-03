var reportInterval;
function init() {
	console.log('started v4');
	showReport();
	reportInterval = setInterval(showReport, 5000);
	var insjob = $('.insert-job');
	$('.ok', insjob).click(() => insjob.data('onOk')());
	$('.cancel', insjob).click(() => insjob.data('onCancel')());
	$('.insert-job').hide();
}
var data;

async function showReport() {
	var txt = await $.get('../report').promise();
	data = JSON.parse(txt);
	var tbody = $('.workers tbody');
	tbody.empty();
	for (var i = 0; i < data.workers.length; i++) {
		let d = data.workers[i];
		let job = d.job || { operation: '-', par1: '-', par2: '-', par3: '-', status: '-', priority: '-', nretry: '-' };
		let div = $('<div/>').addClass('job-desc').appendTo('.workers');
		var row = $(`<tr>
			<td class="tag">${d.tag}</td>
			<td class="priority">${job.priority}</td>
			<td class="nretry">${job.nretry}</td>
			<td class="operation">${job.operation}</td>
			<td class="par1">${job.par1}</td>
			<td class="par2">${job.par2}</td>
			<td class="par3">${job.par3}</td>
			<td class="status">${d.status}</td>
		</tr>`).appendTo(tbody);
		row.attr('title', JSON.stringify(d, null, 2));
	}

	$('.db').empty();
	var nitems = "#job=" + data.job[0].count
		+ ", #done=" + data.done[0].count
		+ ", #error=" + data.error[0].count;

	$('<div class="n-items"/>').text(nitems).appendTo('.db.count');

	data.operation
		.map(x => x.operation + ": " + x.count)
		.map(txt => $('<div/>').text(txt).appendTo('.db.operation'));

	data.status
		.map(x => x.queue + ':' + x.status + ": " + x.count)
		.map(txt => $('<div/>').text(txt).appendTo('.db.status'));

	$('.connections').empty();
	data.connections.map(txt => $('<div/>').text(txt).appendTo('.connections'))
}

function exit() {
	$.get('exit').then(() => alert("done!"));

}

function setN() {
	var n = prompt("quanti workers?");
	if (n === null)
		return;
	n = n - 0;
	console.log("setn " + n);
	$.get('setn/' + n).then(() => console.log("set n=" + n + ", done!"));
}

function insertJob() {
	var fields = ['queue', 'priority', 'operation', 'par1', 'par2', 'par3', 'parentJob', 'extra'];
	var defaults = ['anas-etl', '1000', 'prova', '', '', '', '', ''];

	var panel = $('.insert-job');
	$('.main').hide();
	panel.show();
	var container = $('.form', panel);

	if (!container.data('initialized')) {
		container.data('initialized', true);
		container.empty();
		for (var i = 0; i < fields.length; i++) {
			let field = fields[i];
			$(`<div class="label">${field}</div>`).appendTo(container);
			$(`<div class="finput ${field}"><input type="text" value="${defaults[i]}">`).appendTo(container);
		}
		panel.data('onOk', async function () {
			var params = []
			for (var field of fields) {
				var val = encodeURIComponent($(`.finput.${field} input`, container).val());
				params.push(field + '=' + val);
			}
			var url = 'insertJob?' + params.join('&');
			alert(url);
			panel.hide();
			$.get(url).promise().then(
				ok => alert(JSON.stringify(JSON.parse(ok), null, 2)),
				error => alert(error)
			);
			$('.main').show();
		});
		panel.data('onCancel', function () {
			$('.main').show();
			panel.hide();
		});
	}
}


function startScan() {
	var os = prompt("Obect Store Name", "");
	if (!os)
		return;
	var path = prompt("Path", "/");
	if (!path)
		return;
	var maxrecursion = prompt("max recursion", "100");
	if (!maxrecursion)
		return;
	maxrecursion = maxrecursion - 0;
	var withdoc = prompt("inclusi i documenti?\n0=NO 1=SI", "0");
	if (!withdoc)
		return;
	withdoc = withdoc == '1';
	if (withdoc) {
		var withcontent = prompt("inclusi i content?\n0=NO 1=SI", "0");
		if (!withcontent)
			return;
		withcontent = withcontent == '1';
	}
	var msg = JSON.stringify({ os, path, maxrecursion, withdoc, withcontent }, null, 4);
	var ok = confirm(msg);
	if (!ok)
		return;
	var data = {
		queue: 'anas-etl',
		operation: 'getFolderMD',
		priority: '1000',
		par1: os,
		par2: path,
		extra: JSON.stringify({ maxrecursion, withdoc, withcontent })
	}
	var params = [];
	for (var field in data) {
		var val = encodeURIComponent(data[field]);
		params.push(field + '=' + val);
	}
	var url = 'insertJob?' + params.join('&');
	var ok = confirm(url);
	if (!ok)
		return;
	$.get(url).promise().then(
		ok => alert(JSON.stringify(JSON.parse(ok), null, 2)),
		error => alert(error)
	);

}

function startScanProgetti() {
	var os = "PDM";
	var path = "/wbs/progetti";
	var maxrecursion = 1;
	var withdoc = 0;
	var withcontent = 0;
	var msg = JSON.stringify({ os, path, maxrecursion, withdoc, withcontent }, null, 4);
	var ok = confirm(msg);
	if (!ok)
		return;
	var data = {
		queue: 'anas-etl',
		operation: 'getFolderMD',
		priority: '1000',
		par1: os,
		par2: path,
		extra: JSON.stringify({ maxrecursion, withdoc, withcontent })
	}
	var params = [];
	for (var field in data) {
		var val = encodeURIComponent(data[field]);
		params.push(field + '=' + val);
	}
	var url = 'insertJob?' + params.join('&');
	$.get(url).promise().then(
		ok => alert(JSON.stringify(JSON.parse(ok), null, 2)),
		error => alert(error)
	);
}
$(init);