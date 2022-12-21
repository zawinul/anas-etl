var reportTimeout;
var reportIntervalTime = 20000;

var webSocket = new WebSocket("ws://" + location.hostname + ":" + location.port + "/monitor");
webSocket.onmessage = function (msg) { 
	var obj = JSON.parse(msg.data);
	if (obj['update-worker'])
		updateWorker(obj['update-worker']);
	else
		console.log(obj); 
	
};
webSocket.onclose = function () { alert("WebSocket connection closed") };

function init() {
	console.log('started v4');
	showReport();
	reportTimeout = setInterval(showReport, reportIntervalTime);
	var insjob = $('.insert-job');
	$('.ok', insjob).click(() => insjob.data('onOk')());
	$('.cancel', insjob).click(() => insjob.data('onCancel')());
	$('.insert-job').hide();
}
var data;

function makeWorkerRow(worker) {
	let job = worker && worker.job ? worker.job : { queue:'-', operation: '-', key1: '-', key2: '-', key3: '-', lock: '-', priority: '-', nretry: '-' };
	let tr = $(`<tr>
			<td class="tag">${worker.tag}</td>
			<td class="queue">${job.queue}</td>
			<td class="priority">${job.priority}</td>
			<td class="nretry">${job.nretry}</td>
			<td class="operation">${job.operation}</td>
			<td class="par1">${job.key1}</td>
			<td class="par2">${job.key2}</td>
			<td class="par3">${job.key3}</td>
			<td class="lock">${worker!=null ? worker.status : ''}</td>
		</tr>`);
	tr.attr('tag', worker.tag);
	return tr;
}

function updateWorker(worker) {
	let old = $(`tr[tag="${worker.tag}"]`);
	if (old.length==0)
		return;
	let tr = makeWorkerRow(worker);
	old[0].innerHTML = tr[0].innerHTML;
}

let lastData = null, lastDataTime=null;
async function showReport() {
	clearTimeout(reportTimeout);
	let thisTime = new Date().getTime();
	var txt = await $.get('../report').promise();
	data = JSON.parse(txt);
	if (lastData!=null)
		computeDelta(data, lastData);

	var tbody = $('.workers tbody');
	tbody.empty();
	for (var i = 0; i < data.workers.length; i++) {
		if (data.workers[i]==null)
			continue;
		let div = $('<div/>').addClass('job-desc').appendTo('.workers');
		var row = makeWorkerRow(data.workers[i]);
		row.appendTo(tbody);
		row.attr('title', JSON.stringify(data.workers[i], null, 2));
	}

	$('.db.operation .cell').empty();
	$('<b style="margin-right:20px">to do: '+data.job[0].count+'</b>').appendTo('.todo-operation');
	$('<b style="margin-right:20px">done: '+data.done[0].count+'</b>').appendTo('.done-operation');
	$('<b style="margin-right:20px">error: '+data.error[0].count+'</b>').appendTo('.error-operation');
	function row2text(x) {
		var ds = '';
		if (x.delta) {
			ds=' ('+(x.delta*60000/(thisTime-lastDataTime)).toFixed(1)+' min)';
		}
		return x.queue+" "+x.operation + ": " + x.count+ds;
	}
	data.operation
		.map(row2text)
		.map(txt => $('<div/>').text(txt).appendTo('.todo-operation'));

	data.done_operation
		.map(row2text)
		.map(txt => $('<div/>').text(txt).appendTo('.done-operation'));

	data.error_operation
		.map(row2text)
		.map(txt => $('<div/>').text(txt).appendTo('.error-operation'));

	// $('<b style="margin-right:20px">by status: </b>').appendTo('.db.status');
	// data.status
	// 	.map(x => x.queue + ':' + x.locktag + ": " + x.count)
	// 	.map(txt => $('<div/>').text(txt).appendTo('.db.status'));

	let ref = $('button.refresh');
	ref.clearQueue().animate({
			borderWidth: 1
		}, {
		duration:reportIntervalTime+1000,
		easing: 'linear',
		progress:function(animation, prog, remaining) {
			let pc=100-prog*100;
			ref.css('background', `linear-gradient(90deg, rgba(198,198,198,1) 0%, rgba(220,220,220,1) ${pc}%, rgba(240,240,240,1) ${pc}%)`)
		}
	});
	$('.connections').empty();
	$('<b style="margin-right:20px">connessioni DB: </b>').appendTo('.connections');

	data.connections.map(txt => $('<div/>').text(txt).appendTo('.connections'));
	lastData = data;
	lastDataTime = thisTime;
	reportTimeout = setTimeout(showReport, reportIntervalTime);
}

function computeDelta(data, data2) {
	function cd(arr1,arr2) {
		for(a of arr1) {
			for(b of arr2) {
				if (a.queue==b.queue && a.operation==b.operation)
					a.delta = a.count-b.count;
			}
		}
	}
	cd(data.operation, data2.operation);
	cd(data.done_operation, data2.done_operation);
	cd(data.error_operation, data2.error_operation);
	data.operation.map(x => x.queue+" "+x.operation + ": " + x.count)

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
	setTimeout(showReport, 1000);
}

async function getListeDbs() {
	var os = "PDM";

	var path = "/dbs/lavori";
	var folderId = "{A1B84F73-3F41-4BC7-843B-C21EBD1A1829}";
	launch(folderId, "lavori"); 
	var path = "/dbs/progetti";
	var folderId = "{CD703774-E2DB-494C-98CE-6B8618E6A761}";
	launch(folderId, "progetti"); 


	function launch(folderId, tag) {
		let job = {
			queue: 'qdata',
			operation: 'getListaDbs',
			priority: 100,
			key1: tag,
			key2: folderId,
			key3: tag,
			os, 
			folderId,
			maxrecursion: 100, 
			withdoc: 0, 
			withcontent: 0 
		};
		$.post({url:'insertJob', data:JSON.stringify(job)}).promise().then(
			ok => console.log(JSON.stringify(JSON.parse(ok), null, 2)),
			error => alert(error)
		);
	}
}

// async function getProgettoSingolo() {
// 	var os = "PDM";

// 	var path = "/dbs/progetti/LO.710.E.D.19.01";
// 	var folderId = "{B35E558C-8317-4260-985B-C2B93B7B8A5A}";
// 	let job = {
// 		queue: 'qdata',
// 		operation: 'getFolderMD',
// 		priority: 10000,
// 		key1: os,
// 		key2: path,
// 		key3: "0",
// 		os, 
// 		folderId,
// 		maxrecursion: 100, 
// 		withdoc: 1, 
// 		withcontent: 0 

// 	};
// 	$.post({url:'insertJob', data:JSON.stringify(job)}).promise().then(
// 		ok => console.log(JSON.stringify(JSON.parse(ok), null, 2)),
// 		error => alert(error)
// 	);
	
// }


function startScanArchivi() {
	var withdoc = confirm("with doc");
	var withcontent = withdoc && confirm("with content");

	let job = {
		queue: 'qdata',
		operation: 'startScanArchivi',
		priority:1000000,
		withdoc, 
		withcontent,
		buildDir: false
	};

	var msg = JSON.stringify(job, null, 4);
	if (!confirm(msg))
		return;

	$.post({url:'insertJob', data:JSON.stringify(job)}).promise().then(
		ok => alert(JSON.stringify(JSON.parse(ok), null, 2)),
		error => alert(error)
	);
}

function startScanArchivi2() {
	var withcontent = confirm("with content");

	let job = {
		queue:'qdata',
		operation: 'startScanArchivi2',
		priority:1000000,
		withcontent,
		buildDir: false
	};

	var msg = JSON.stringify(job, null, 4);
	if (!confirm(msg))
		return;

	$.post({url:'insertJob', data:JSON.stringify(job)}).promise().then(
		ok => alert(JSON.stringify(JSON.parse(ok), null, 2)),
		error => alert(error)
	);
}


function startScanArchiviNode() {
	var withcontent = confirm("with content");
	var path = prompt("path", "AN.8");
	if (!path)
		return;
	var priority = prompt("priority", "0");
	if (priority===null || priority===undefined)
		return;
	var dir = path.substring(0, path.lastIndexOf("."));
	let job = {
		queue:'qdata',
		operation: 'startArchiviNode',
		priority:priority-0,
		dir,
		withcontent,
		path,
		buildDir: false
	};

	var msg = JSON.stringify(job, null, 4);
	if (!confirm(msg))
		return;

	$.post({url:'insertJob', data:JSON.stringify(job)}).promise().then(
		ok => alert(JSON.stringify(JSON.parse(ok), null, 2)),
		error => alert(error)
	);
}


function startScanProgetti() { startScanDBS('progetti'); }
function startScanLavori() { startScanDBS('lavori'); }

function startScanDBS(what) {
	var withdoc = confirm("with doc");
	var withcontent = withdoc && confirm("with content");

	let job = {
		queue:'qdata',
		operation: 'startScanDBS',
		priority:2000000,
		key1: what,
		withdoc, 
		withcontent,
		buildDir: true
	};

	var msg = JSON.stringify(job, null, 4);
	if (!confirm(msg))
		return;

	$.post({url:'insertJob', data:JSON.stringify(job)}).promise().then(
		ok => alert(JSON.stringify(JSON.parse(ok), null, 2)),
		error => alert(error)
	);
}

async function sql(query) {
	$.post({url:'sql', data:query}).promise().then(
		ok => console.log(ok),
		error => alert(error)
	);
}


$(init);