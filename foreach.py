from metaflow import FlowSpec, step


class ForEachFlow(FlowSpec):

    data = [i for i in range(50)]

    @step
    def start(self):
        self.next(self.feprint, foreach='data')

    @step
    def feprint(self):
        import os
        self.c = self.input
        print(f"process {os.getpid()} printing {self.c}")
        self.next(self.join)


    @step
    def join(self, inputs):
        print(f"joining {inputs}...")
        print(f"feprint  {[input.c for input in inputs]}")
        self.next(self.end)

    @step
    def end(self):
        print("Done!")




if __name__ == "__main__":
    ForEachFlow()