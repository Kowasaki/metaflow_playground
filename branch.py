from metaflow import FlowSpec, step


class BranchFlow(FlowSpec):

    data = [i for i in range(50)]

    @step
    def start(self):
        self.next(self.pprint, self.pprint2, self.pprint3)

    @step
    def pprint(self):
        from random import choice
        self.c = choice(self.data)
        print(self.c)
        self.next(self.join)

    @step
    def pprint2(self):
        from random import choice
        self.c = choice(self.data)
        print(self.c)
        self.next(self.join)

    @step
    def pprint3(self):
        from random import choice
        self.c = choice(self.data)
        print(self.c)
        self.next(self.join)

    @step
    def join(self, inputs):
        print(f"joining {inputs}...")
        print(f"pprint chose {inputs.pprint.c}")
        print(f"pprint2 chose {inputs.pprint2.c}")
        print(f"pprint3 chose {inputs.pprint3.c}")
        self.next(self.end)

    @step
    def end(self):
        print("Done!")




if __name__ == "__main__":
    BranchFlow()