use clap::Parser;
use flume::{Receiver, Sender};
use std::{
    io::{BufRead, BufReader, Read},
    process,
};

#[derive(Parser)]
pub struct Args {}

struct Process {
    name: String,
    pid: u32,
    out_rx: Receiver<String>,
    proc: process::Child,
}

impl Process {
    pub fn spawn(name: impl ToString, args: &[&str]) -> Self {
        let (out_tx, out_rx) = flume::bounded(128);
        let bin = args[0];
        let args = args[1..].iter().map(std::ffi::OsStr::new);
        let name = name.to_string();

        tracing::debug!("spawning {bin} as {name:?}");

        let mut proc = process::Command::new(bin)
            // .args(args.split(' '))
            .args(args)
            .stdout(process::Stdio::piped())
            .stderr(process::Stdio::piped())
            .spawn()
            .expect("start process");

        fn fwd_stream(stream: Option<impl Read + Send + 'static>, tx: Sender<String>) {
            if let Some(stream) = stream {
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(stream);
                    let mut line = String::new();
                    loop {
                        match reader.read_line(&mut line) {
                            Err(err) => {
                                eprintln!("error reading line: {err}");
                            }
                            Ok(0) => {
                                break;
                            }
                            Ok(_) => {
                                let _ = tx.send(line.trim().to_string());
                                line.clear();
                            }
                        }
                    }
                });
            }
        }

        fwd_stream(proc.stdout.take(), out_tx.clone());
        fwd_stream(proc.stderr.take(), out_tx);

        let pid = proc.id();

        Self {
            proc,
            pid,
            out_rx,
            name,
        }
    }

    #[allow(dead_code)]
    pub fn sigterm(&self) {
        tracing::debug!("sending sigterm to {} ({})", self.name, self.pid);
        sigterm(self.pid);
    }

    pub fn wait(&mut self) {
        self.proc.wait().expect("wait for process");
    }
}

pub fn sigterm(pid: u32) {
    tracing::debug!("sending sigterm to {}", pid);
    process::Command::new("kill")
        .arg("-SIGTERM")
        .arg(pid.to_string())
        .spawn()
        .expect("send sigterm")
        .wait()
        .expect("wait for sigterm");
}

fn print_all_outputs(mut names: Vec<String>, mut receivers: Vec<Receiver<String>>) {
    enum Action {
        Print { name: String, content: String },
        RemoveReceiver(usize),
    }

    let name_padding = names.iter().map(|n| n.len()).max().unwrap_or(0);

    while !receivers.is_empty() {
        let mut sel = flume::Selector::new();

        for (i, (name, rx)) in names.iter().zip(&receivers).enumerate() {
            sel = sel.recv(rx, move |line| {
                line.map(|l| Action::Print {
                    content: l,
                    name: name.clone(),
                })
                .unwrap_or(Action::RemoveReceiver(i))
            });
        }

        let action = sel.wait();

        match action {
            Action::Print { name, content } => {
                println!(
                    "[{name}]{:width$} {content}",
                    "",
                    width = name_padding - name.len()
                );
            }
            Action::RemoveReceiver(i) => {
                receivers.remove(i);
                names.remove(i);
            }
        }
    }
}

pub fn run(_args: Args) {
    let bin = std::env::current_exe().expect("get current exe");

    let worker_procs = {
        let worker_types = ["add", "add", "add", "mul", "mul"];
        let cmds = worker_types
            .iter()
            .map(|name| format!("{} worker -t {name}", bin.display()))
            .collect::<Vec<_>>();
        let names = worker_types
            .iter()
            .enumerate()
            .map(|(i, name)| format!("{name}-{}", i + 1))
            .collect::<Vec<_>>();

        names
            .iter()
            .zip(&cmds)
            .map(|(name, cmd)| Process::spawn(name, &cmd.split(' ').collect::<Vec<_>>()))
            .collect::<Vec<_>>()
    };

    let producer_procs = {
        let names = ["producer-1", "producer-2"];
        let cmds = names
            .iter()
            .map(|_name| format!("{} producer", bin.display()))
            .collect::<Vec<_>>();
        names
            .iter()
            .zip(&cmds)
            .map(|(name, cmd)| Process::spawn(name, &cmd.split(' ').collect::<Vec<_>>()))
            .collect::<Vec<_>>()
    };

    let procs = worker_procs
        .into_iter()
        .chain(producer_procs)
        .collect::<Vec<_>>();

    let pids = procs.iter().map(|p| p.pid).collect::<Vec<_>>();
    ctrlc::set_handler(move || {
        tracing::info!("got ctrl-c");
        for stop in &pids {
            sigterm(*stop);
        }
    })
    .expect("set ctrl-c handler");

    let (names, receivers) = procs
        .iter()
        .map(|p| (p.name.clone(), p.out_rx.clone()))
        .unzip::<_, _, Vec<_>, Vec<_>>();

    // blocks
    print_all_outputs(names, receivers);

    for mut proc in procs {
        proc.wait();
    }

    println!("done");
}
