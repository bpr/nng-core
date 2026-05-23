//! Turn-based tic-tac-toe over PAIR0, with the typestate pattern enforcing
//! whose turn it is.
//!
//! ```text
//! Game<MyTurn>       ──play(square)──▶  AfterMove<OpponentTurn>
//! Game<OpponentTurn> ──wait_for_move──▶ AfterMove<MyTurn>
//!
//! AfterMove<NextState> = Continue(Game<NextState>) | GameOver
//! ```
//!
//! Each transition method consumes `self`, so the *previous* turn-state
//! disappears the moment you act on it.  Calling `play` on `Game<OpponentTurn>`
//! is a compile error — that method literally does not exist on that type:
//!
//! ```ignore
//! let game = Game::<OpponentTurn>::join(addr).await?;
//! game.play(0).await?;            // ❌ no `play` on Game<OpponentTurn>
//! ```
//!
//! Compare this to the network-error version: without typestate the
//! server would have to refuse mid-game with a "not your turn" reply, and
//! tests would have to cover that path.  Here the rejection happens at
//! `cargo check` and never reaches the wire.
//!
//! Run with two terminals:
//! ```text
//! cargo run --example tictactoe -- --host
//! cargo run --example tictactoe -- --join
//! ```

use nng_core::{Message, socket::pair0::Pair0};
use std::{env, marker::PhantomData};

const ADDR: &str = "tcp://127.0.0.1:5571";

type Err = Box<dyn std::error::Error + Send + Sync>;

// ── Board ────────────────────────────────────────────────────────────────────

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Cell {
    Empty,
    X,
    O,
}

#[derive(Copy, Clone)]
pub struct Board {
    cells: [Cell; 9],
}

impl Board {
    fn new() -> Self {
        Self {
            cells: [Cell::Empty; 9],
        }
    }

    fn place(&mut self, sq: usize, mark: Cell) -> Result<(), &'static str> {
        if sq > 8 {
            return Err("invalid square");
        }
        if self.cells[sq] != Cell::Empty {
            return Err("square already taken");
        }
        self.cells[sq] = mark;
        Ok(())
    }

    fn winner(&self) -> Option<Cell> {
        const LINES: [[usize; 3]; 8] = [
            [0, 1, 2],
            [3, 4, 5],
            [6, 7, 8],
            [0, 3, 6],
            [1, 4, 7],
            [2, 5, 8],
            [0, 4, 8],
            [2, 4, 6],
        ];
        for [a, b, c] in LINES {
            if self.cells[a] != Cell::Empty
                && self.cells[a] == self.cells[b]
                && self.cells[b] == self.cells[c]
            {
                return Some(self.cells[a]);
            }
        }
        None
    }

    fn full(&self) -> bool {
        self.cells.iter().all(|c| *c != Cell::Empty)
    }

    /// Index of the first empty square, if any.
    pub fn first_empty(&self) -> Option<usize> {
        self.cells.iter().position(|c| *c == Cell::Empty)
    }
}

impl std::fmt::Display for Board {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for row in 0..3 {
            for col in 0..3 {
                let c = match self.cells[row * 3 + col] {
                    Cell::X => 'X',
                    Cell::O => 'O',
                    Cell::Empty => '.',
                };
                write!(f, " {c} ")?;
                if col < 2 {
                    write!(f, "|")?;
                }
            }
            writeln!(f)?;
            if row < 2 {
                writeln!(f, "-----------")?;
            }
        }
        Ok(())
    }
}

// ── State markers ────────────────────────────────────────────────────────────

pub struct MyTurn;
pub struct OpponentTurn;

// ── Session ──────────────────────────────────────────────────────────────────

pub struct Game<S> {
    pair: Pair0,
    board: Board,
    me: Cell,
    _state: PhantomData<S>,
}

impl<S> Game<S> {
    pub fn board(&self) -> &Board {
        &self.board
    }
}

/// Result of a turn transition: continue (with the next state) or game over.
///
/// `GameOver` carries the final board so the caller can render it after
/// the `Game` struct has been consumed by the terminal transition.
pub enum AfterMove<NextState> {
    Continue(Game<NextState>),
    GameOver {
        outcome: GameOver,
        final_board: Board,
    },
}

pub enum GameOver {
    YouWon,
    YouLost,
    Draw,
}

impl Game<MyTurn> {
    /// Listen, become X, and move first.  Blocks until an opponent dials.
    pub async fn host(addr: &str) -> Result<Self, Err> {
        Ok(Self {
            pair: Pair0::listen(addr).await?,
            board: Board::new(),
            me: Cell::X,
            _state: PhantomData,
        })
    }

    /// Place my mark on `sq` and send it to the opponent.  Consumes
    /// `self`; the only way to land back on `Game<MyTurn>` is via
    /// `wait_for_move` returning `Continue(...)`.
    pub async fn play(mut self, sq: usize) -> Result<AfterMove<OpponentTurn>, Err> {
        self.board.place(sq, self.me)?;
        let mut msg = Message::new();
        msg.push_back(&[sq as u8]);
        self.pair.send(msg).await?;
        Ok(self.classify())
    }

    fn classify(self) -> AfterMove<OpponentTurn> {
        if let Some(w) = self.board.winner() {
            let outcome = if w == self.me {
                GameOver::YouWon
            } else {
                GameOver::YouLost
            };
            return AfterMove::GameOver {
                outcome,
                final_board: self.board,
            };
        }
        if self.board.full() {
            return AfterMove::GameOver {
                outcome: GameOver::Draw,
                final_board: self.board,
            };
        }
        AfterMove::Continue(Game {
            pair: self.pair,
            board: self.board,
            me: self.me,
            _state: PhantomData,
        })
    }
}

impl Game<OpponentTurn> {
    /// Dial the host as O.
    pub async fn join(addr: &str) -> Result<Self, Err> {
        Ok(Self {
            pair: Pair0::dial(addr).await?,
            board: Board::new(),
            me: Cell::O,
            _state: PhantomData,
        })
    }

    /// Block on the opponent's move, apply it, decide whether the game
    /// continues.  Consumes `self`.
    pub async fn wait_for_move(mut self) -> Result<AfterMove<MyTurn>, Err> {
        let msg = self.pair.recv().await?;
        let sq = *msg.body().first().ok_or("empty move frame")? as usize;
        let opp = match self.me {
            Cell::X => Cell::O,
            Cell::O => Cell::X,
            Cell::Empty => unreachable!(),
        };
        self.board.place(sq, opp)?;
        Ok(self.classify())
    }

    fn classify(self) -> AfterMove<MyTurn> {
        if let Some(w) = self.board.winner() {
            let outcome = if w == self.me {
                GameOver::YouWon
            } else {
                GameOver::YouLost
            };
            return AfterMove::GameOver {
                outcome,
                final_board: self.board,
            };
        }
        if self.board.full() {
            return AfterMove::GameOver {
                outcome: GameOver::Draw,
                final_board: self.board,
            };
        }
        AfterMove::Continue(Game {
            pair: self.pair,
            board: self.board,
            me: self.me,
            _state: PhantomData,
        })
    }
}

// ── Demo driver ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Err> {
    match env::args().nth(1).as_deref() {
        Some("--host") => run_host().await,
        Some("--join") => run_join().await,
        _ => {
            eprintln!("usage: tictactoe --host | --join");
            Ok(())
        }
    }
}

fn announce(role: &str, outcome: GameOver, final_board: &Board) {
    println!("{final_board}");
    let label = match outcome {
        GameOver::YouWon => "WIN",
        GameOver::YouLost => "LOSS",
        GameOver::Draw => "DRAW",
    };
    println!("{role}: {label}");
}

/// Host's scripted strategy: take the diagonal 0 → 4 → 8.
async fn run_host() -> Result<(), Err> {
    let mut game = Game::<MyTurn>::host(ADDR).await?;
    println!("host: opponent joined; I'm X (script: 0, 4, 8)\n");

    for sq in [0usize, 4, 8] {
        println!("host: play {sq}");
        let game_o = match game.play(sq).await? {
            AfterMove::Continue(g) => g,
            AfterMove::GameOver {
                outcome,
                final_board,
            } => {
                announce("host", outcome, &final_board);
                return Ok(());
            }
        };
        println!("{}", game_o.board());

        game = match game_o.wait_for_move().await? {
            AfterMove::Continue(g) => g,
            AfterMove::GameOver {
                outcome,
                final_board,
            } => {
                announce("host", outcome, &final_board);
                return Ok(());
            }
        };
        println!("{}", game.board());
    }
    Ok(())
}

/// Join's strategy: play whatever square is empty first.  Loses cleanly
/// to the diagonal script above.
async fn run_join() -> Result<(), Err> {
    let mut game = Game::<OpponentTurn>::join(ADDR).await?;
    println!("join: connected; I'm O\n");

    loop {
        let game_my = match game.wait_for_move().await? {
            AfterMove::Continue(g) => g,
            AfterMove::GameOver {
                outcome,
                final_board,
            } => {
                announce("join", outcome, &final_board);
                return Ok(());
            }
        };
        println!("{}", game_my.board());

        let sq = game_my.board().first_empty().ok_or("no empty squares")?;
        println!("join: play {sq}");
        game = match game_my.play(sq).await? {
            AfterMove::Continue(g) => g,
            AfterMove::GameOver {
                outcome,
                final_board,
            } => {
                announce("join", outcome, &final_board);
                return Ok(());
            }
        };
        println!("{}", game.board());
    }
}
