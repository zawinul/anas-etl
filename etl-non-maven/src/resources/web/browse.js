const browse = function() {
	let base = 'http://127.0.0.1:6160/'
	let curdir;

	function init() {
		getDir('/'); 
	}

	function getDir(dir) {
		console.log('getDir '+dir);
		$.get(base+'getdir2?path='+dir).promise().then(function(data) {
			curdir=dir;
			console.log({data, curdir});
			let d = JSON.parse(data);
			let dirs = d[0];
			let files = d[1];
			$('.dirs').empty();
			$('.files').empty();
			for(child of dirs)
				$('.dirs').append(buildDirLink(child));
			for(file of files)
				$('.files').append(buildFileLink(file));
		});
	}

	function buildDirLink(dir) {
		let path = curdir=='/' 
			? curdir+dir 
			: curdir+'/'+dir;
		let a = $(`<a class="dir" href="#" title="${path}">${dir}</a>`);
		console.log({path123:path})
		a.click(()=>getDir(path));
		return a;
	}

	
	function buildFileLink(file) {
		let name = file.split(':')[0];
		let comment = file.split(':')[1];
		let path = curdir=='/' 
			? curdir+name 
			: curdir+'/'+name;
		let div = $(`<dir class="file">
		<a class="file-name" target="file" href="${base+"getfile?path="+path}" title="${path}">${name}</a>
		<span class="file-comment">${comment}</span>
		</div>`);
		return div;
	}

	return {
		init
	}
}();

browse.init();